import isPlainObject from 'lodash/isPlainObject'
import snakeCase from 'lodash/snakeCase'
import {
    StorageRegistry,
    CollectionDefinition,
    isChildOfRelationship,
    Relationship,
    CollectionField,
} from '@worldbrain/storex'
import * as backend from '@worldbrain/storex/lib/types/backend'
import { StorageBackendFeatureSupport } from '@worldbrain/storex/lib/types/backend-features'
import * as typeorm from 'typeorm'
import {
    Connection,
    ConnectionOptions,
    createConnection,
    EntitySchema,
} from 'typeorm'
import { collectionsToEntitySchemas, AutoPkOptions } from './entities'
import {
    cleanOptionalFieldsForRead,
    ObjectCleaner,
    makeCleanerChain,
    makeCleanBooleanFieldsForRead,
    cleanRelationshipFieldsForWrite,
    cleanRelationshipFieldsForRead,
    supportsJsonFields,
    serializeJsonFields,
    deserializeJsonFields,
} from './utils'
import { ComplexCreateMiddleware } from './middleware'

const OPERATORS_AS_STRINGS = {
    $lt: '<',
    $lte: '<=',
    $gt: '>',
    $gte: '>=',
    $ne: '!=',
    $eq: '=',
    $in: 'IN',
}

interface InternalOperationOptions {
    entityManager?: typeorm.EntityManager
}

export class TypeORMStorageBackend extends backend.StorageBackend {
    features: StorageBackendFeatureSupport = {
        count: true,
        createWithRelationships: true,
        fullTextSearch: false,
        executeBatch: true,
        transaction: false,
        singleFieldSorting: true,
        resultLimiting: true,
    }

    public connection?: Connection
    public entitySchemas?: { [collectionName: string]: EntitySchema }
    private readObjectCleaner!: ObjectCleaner
    private writeObjectCleaner!: ObjectCleaner
    private initialized = false

    constructor(
        private options: {
            connectionOptions: ConnectionOptions
            autoPkOptions?: AutoPkOptions
            forceNewConnection?: boolean
            legacyMemexCompatibility?: boolean // temporary option that prevents normalisation of optional fields
        },
    ) {
        super()
    }

    configure({ registry }: { registry: StorageRegistry }) {
        super.configure({ registry })
        registry.once('initialized', this._onRegistryInitialized)
    }

    _onRegistryInitialized = async () => {
        this.initialized = true
        this.entitySchemas = collectionsToEntitySchemas(this.registry, {
            databaseType: this.options.connectionOptions.type,
            autoPkOptions: this.options.autoPkOptions || {
                generated: true,
                type: 'integer',
            },
        })
        if (this.options.forceNewConnection) {
            try {
                const existingConnection = typeorm
                    .getConnectionManager()
                    .get('default')
                if (existingConnection) {
                    await existingConnection.close()
                }
            } catch (e) {
                if (e.name !== 'ConnectionNotFoundError') {
                    throw e
                }
            }
        }
        this.connection = await createConnection({
            ...this.options.connectionOptions,
            entities: Object.values(this.entitySchemas),
        })
        this.readObjectCleaner = makeCleanerChain([
            ...(!supportsJsonFields(this.options.connectionOptions.type)
                ? [deserializeJsonFields]
                : []),
            makeCleanBooleanFieldsForRead({ storageRegistry: this.registry }),
            ...(this.options.legacyMemexCompatibility
                ? [cleanOptionalFieldsForRead]
                : []),
            cleanRelationshipFieldsForRead,
        ])
        this.writeObjectCleaner = makeCleanerChain([
            ...(!supportsJsonFields(this.options.connectionOptions.type)
                ? [serializeJsonFields]
                : []),
            cleanRelationshipFieldsForWrite,
        ])
    }

    async migrate(options: { database?: string } = {}) {
        await this.connection!.synchronize()
    }

    async cleanup(): Promise<any> { }

    async createObject(
        collection: string,
        object: any,
        options: backend.CreateSingleOptions & InternalOperationOptions = {},
    ): Promise<backend.CreateSingleResult> {
        const { repository, collectionDefinition } = this._preprocessOperation(
            collection,
            options,
        )
        const cleanedObject = this.writeObjectCleaner(object, {
            collectionDefinition,
        })
        const savedObject = await repository.save(cleanedObject)

        return {
            object: this.readObjectCleaner(savedObject, {
                collectionDefinition,
            }),
        }
    }

    async findObjects<T>(
        collection: string,
        where: any,
        options: backend.FindManyOptions = {},
    ): Promise<Array<T>> {
        const {
            collectionDefinition,
            queryBuilderWithWhere,
        } = this._preprocessFilteredOperation(collection, where, {
            ...options,
            tableCasing: 'camel-case',
        })
        const objects = await queryBuilderWithWhere
            .orderBy(convertOrder(options.order || [], { collection }))
            .skip(options.skip)
            .take(options.limit)
            .getMany()

        return objects.map(object =>
            this.readObjectCleaner(object, { collectionDefinition }),
        )
    }

    async updateObjects(
        collection: string,
        where: any,
        updates: any,
        options: backend.UpdateManyOptions & InternalOperationOptions = {},
    ): Promise<backend.UpdateManyResult> {
        const {
            queryBuilderWithWhere,
            collectionDefinition,
        } = this._preprocessFilteredOperation(collection, where, {
            ...options,
            tableCasing: 'snake-case',
        })
        const convertedUpdates = this.writeObjectCleaner(updates, {
            collectionDefinition,
        })
        await queryBuilderWithWhere
            .update()
            .set(convertedUpdates)
            .execute()
    }

    async deleteObjects(
        collection: string,
        where: any,
        options: backend.DeleteManyOptions & InternalOperationOptions = {},
    ): Promise<backend.DeleteManyResult> {
        const { queryBuilderWithWhere } = this._preprocessFilteredOperation(
            collection,
            where,
            {
                ...options,
                tableCasing: 'camel-case',
            },
        )
        await queryBuilderWithWhere.delete().execute()
    }

    async countObjects(
        collection: string,
        where: any,
        options: backend.CountOptions = {},
    ): Promise<number> {
        const {
            collectionDefinition,
            queryBuilderWithWhere,
        } = this._preprocessFilteredOperation(collection, where, {
            ...options,
            tableCasing: 'snake-case',
        })
        return queryBuilderWithWhere.getCount()
    }

    async executeBatch(batch: backend.OperationBatch) {
        if (!batch.length) {
            return { info: {} }
        }

        const info: { [placeholder: string]: { object: any } } = {}
        await this.connection!.transaction(async entityManager => {
            const placeholders: { [key: string]: any } = {}
            for (const operation of batch) {
                if (operation.operation === 'createObject') {
                    const toInsert =
                        operation.args instanceof Array
                            ? operation.args[0]
                            : operation.args
                    for (const { path, placeholder } of operation.replace ||
                        []) {
                        toInsert[path as string] = placeholders[placeholder].id
                    }

                    const { object } = await this.createObject(
                        operation.collection,
                        toInsert,
                        { entityManager },
                    )
                    if (operation.placeholder) {
                        info[operation.placeholder] = { object }
                        placeholders[operation.placeholder] = object
                    }
                } else if (operation.operation === 'updateObjects') {
                    await this.updateObjects(
                        operation.collection,
                        operation.where,
                        operation.updates,
                    )
                } else if (operation.operation === 'deleteObjects') {
                    await this.deleteObjects(
                        operation.collection,
                        operation.where,
                    )
                } else {
                    throw new Error(
                        `Unsupported operation in batch: ${
                        (operation as any).operation
                        }`,
                    )
                }
            }
        })
        return { info }
    }

    async operation(name: string, ...args: any[]) {
        if (!this.initialized) {
            throw new Error(
                'Tried to use TypeORM backend without calling StorageManager.finishInitialization() first',
            )
        }

        return super.operation(name, ...args)
    }

    getRepositoryForCollection(
        collectionName: string,
        options?: InternalOperationOptions & { database?: string },
    ) {
        const entityManager =
            options && options.entityManager
                ? options.entityManager
                : this.connection!
        return entityManager.getRepository(this.entitySchemas![collectionName])
    }

    _preprocessOperation(
        collectionName: string,
        options?: { database?: string },
    ) {
        const repository = this.getRepositoryForCollection(
            collectionName,
            options,
        )
        const collectionDefinition = this.registry.collections[collectionName]
        return { repository, collectionDefinition }
    }

    _preprocessFilteredOperation(
        collectionName: string,
        where: any,
        options: {
            tableCasing: 'camel-case' | 'snake-case'
            database?: string
        },
    ) {
        const { repository, collectionDefinition } = this._preprocessOperation(
            collectionName,
            options,
        )
        const tableAlias = options.tableCasing === 'camel-case' ? collectionName : snakeCase(collectionName)
        const queryBuilder = repository.createQueryBuilder(tableAlias)
        if (Object.keys(where)) {
            const convertedWhere = convertQueryWhere(where, {
                collectionDefinition,
                tableCasing: options.tableCasing,
            })
            const queryBuilderWithWhere = queryBuilder.where(
                convertedWhere.expression,
                convertedWhere.placeholders,
            )
            return {
                repository,
                collectionDefinition,
                queryBuilderWithWhere,
            }
        } else {
            return {
                repository,
                collectionDefinition,
                queryBuilderWithWhere: queryBuilder,
            }
        }
    }
}

function convertQueryWhere(
    where: { [key: string]: any },
    options: {
        collectionDefinition: CollectionDefinition
        tableCasing: 'camel-case' | 'snake-case'
        columnCasing?: 'camel-case' | 'snake-case'
    },
): {
    expression: string
    placeholders: { [key: string]: any }
} {
    options.columnCasing = options.columnCasing || 'camel-case'

    const convertFieldName = (
        fieldName: string,
        relationship?: Relationship,
    ) => {
        let converted: string
        if (relationship) {
            if (isChildOfRelationship(relationship)) {
                converted = relationship.fieldName!
            } else {
                throw new Error(
                    `Not supported yet to filter by this relationship: ${fieldName}`,
                )
            }
        } else {
            converted = fieldName
        }

        return options.columnCasing === 'snake-case'
            ? snakeCase(converted)
            : converted
    }

    const tableName =
        options.tableCasing === 'snake-case'
            ? snakeCase(options.collectionDefinition.name)
            : options.collectionDefinition.name

    const placeholders: { [key: string]: any } = {}
    const expressions: string[] = []
    for (const [fieldName, predicate] of Object.entries(where)) {
        const fieldDefinition: CollectionField | undefined =
            options.collectionDefinition.fields[fieldName]
        const relationship: Relationship | undefined = options
            .collectionDefinition.relationshipsByAlias![fieldName]
        if (!fieldDefinition && !relationship) {
            throw new Error(
                `Tried to filter by non-existing field '${fieldName}'`,
            )
        }

        const conditions: Array<[keyof typeof OPERATORS_AS_STRINGS, any]> = []
        if (isPlainObject(predicate)) {
            for (const [key, value] of Object.entries(predicate)) {
                if (key.charAt(0) === '$') {
                    conditions.push([key as any, value])
                }
            }
        }

        if (!conditions.length) {
            conditions.push(['$eq', predicate])
        } else if (conditions.length > 1) {
            throw new Error(
                `Multiple operators per field in 'where' are not supported yet`,
            )
        }

        let [operator, rhs] = conditions[0]
        let operatorValid = !!OPERATORS_AS_STRINGS[operator]
        if (fieldDefinition && fieldDefinition.type === 'json') {
            if (operator === '$eq') {
                rhs = JSON.stringify(rhs)
            } else {
                operatorValid = false
            }
        }

        if (!operatorValid) {
            throw new Error(
                `Unsupported operator '${operator}' for field '${fieldName}'`,
            )
        }

        const convertedFieldName = convertFieldName(fieldName, relationship)

        // The former used to be necessary, but behavior was inconsistent.
        // Leaving this here in case we need to revert this change
        // const fullQueryField = `${tableName}.${convertedFieldName}`
        const fullQueryField = convertedFieldName

        if (operator === '$eq' && rhs === null) {
            expressions.push(`${fullQueryField} IS NULL`)
        } else if (operator === '$ne' && rhs === null) {
            expressions.push(`${fullQueryField} IS NOT NULL`)
        } else {
            placeholders[convertedFieldName] = rhs
            if (operator === '$in') {
                expressions.push(
                    `${fullQueryField} IN (:...${convertedFieldName})`,
                )
            } else {
                expressions.push(
                    `${fullQueryField} ${OPERATORS_AS_STRINGS[operator]} :${convertedFieldName}`,
                )
            }
        }
    }
    return { expression: expressions.join(' AND '), placeholders }
}

function convertOrder(
    order: [string, 'asc' | 'desc'][],
    options: { collection: string },
) {
    const converted: { [fieldName: string]: 'ASC' | 'DESC' } = {}
    for (const [fieldName, direction] of order) {
        converted[`${options.collection}.${fieldName}`] =
            direction === 'asc' ? 'ASC' : 'DESC'
    }
    return converted
}
