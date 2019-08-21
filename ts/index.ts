import isPlainObject from 'lodash/isPlainObject'
import { StorageRegistry, CollectionDefinition, isChildOfRelationship } from '@worldbrain/storex'
import * as backend from '@worldbrain/storex/lib/types/backend'
import { StorageBackendFeatureSupport } from '@worldbrain/storex/lib/types/backend-features';
import * as typeorm from 'typeorm'
import { Connection, ConnectionOptions, createConnection, EntitySchema } from 'typeorm';
import { collectionsToEntitySchemas } from './entities';
import { cleanOptionalFieldsForRead, ObjectCleaner, makeCleanerChain, makeCleanBooleanFieldsForRead, cleanRelationshipFieldsForWrite, cleanRelationshipFieldsForRead } from './utils';
import { ComplexCreateMiddleware } from './middleware';

const OPERATORS_AS_STRINGS = {
    $lt: '<',
    $lte: '<=',
    $gt: '>',
    $gte: '>=',
    $ne: '!=',
    $eq: '=',
    $in: 'IN'
}

interface InternalOperationOptions {
    entityManager? : typeorm.EntityManager
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

    public connection? : Connection
    public entitySchemas? : {[collectionName : string] : EntitySchema}
    private readObjectCleaner! : ObjectCleaner
    private writeObjectCleaner! : ObjectCleaner
    private initialized = false

    constructor(private options : { connectionOptions : ConnectionOptions }) {
        super()
    }

    configure({ registry } : { registry : StorageRegistry }) {
        super.configure({ registry })
        registry.once('initialized', this._onRegistryInitialized)
    }

    _onRegistryInitialized = async () => {
        this.initialized = true
        this.entitySchemas = collectionsToEntitySchemas(this.registry)
        this.connection = await createConnection({
            ...this.options.connectionOptions,
            entities: Object.values(this.entitySchemas),
        })
        this.readObjectCleaner = makeCleanerChain([
            makeCleanBooleanFieldsForRead({ storageRegistry: this.registry }),
            cleanOptionalFieldsForRead,
            cleanRelationshipFieldsForRead,
        ])
        this.writeObjectCleaner = makeCleanerChain([
            cleanRelationshipFieldsForWrite,
        ])
    }

    async migrate(options : { database? : string } = {}) {
        await this.connection!.synchronize()
    }

    async cleanup(): Promise<any> {

    }

    async createObject(collection : string, object : any, options: backend.CreateSingleOptions & InternalOperationOptions = {}): Promise<backend.CreateSingleResult> {
        const { repository, collectionDefinition } = this._preprocessOperation(collection, options)
        const cleanedObject = this.writeObjectCleaner(object, { collectionDefinition })
        const savedObject = await repository.save(cleanedObject)
        return { object: this.readObjectCleaner(savedObject, { collectionDefinition }) }
    }

    async findObjects<T>(collection : string, where : any, options: backend.FindManyOptions = {}): Promise<Array<T>> {
        const { collectionDefinition, queryBuilderWithWhere } = this._preprocessFilteredOperation(collection, where, options)
        const objects = await queryBuilderWithWhere
            .orderBy(convertOrder(options.order || [], { collection }))
            .take(options.limit)
            .getMany()
        
        return objects.map(object => this.readObjectCleaner(object, { collectionDefinition }))
    }

    async updateObjects(collection : string, where : any, updates : any, options : backend.UpdateManyOptions & InternalOperationOptions = {}): Promise<backend.UpdateManyResult> {
        const { queryBuilderWithWhere } = this._preprocessFilteredOperation(collection, where, options)
        const convertedUpdates = updates
        await queryBuilderWithWhere.update(convertedUpdates).execute()
    }

    async deleteObjects(collection : string, where : any, options : backend.DeleteManyOptions & InternalOperationOptions = {}): Promise<backend.DeleteManyResult> {
        const { queryBuilderWithWhere } = this._preprocessFilteredOperation(collection, where, options)
        await queryBuilderWithWhere.delete().execute()
    }

    async countObjects(collection : string, where : any, options : backend.CountOptions = {}) : Promise<number> {
        const { collectionDefinition, queryBuilderWithWhere } = this._preprocessFilteredOperation(collection, where, options)
        return queryBuilderWithWhere.getCount()
    }

    async executeBatch(batch : backend.OperationBatch) {
        if (!batch.length) {
            return { info: {} }
        }

        const info = {}
        await this.connection!.transaction(async entityManager => {
            const placeholders = {}
            for (const operation of batch) {
                if (operation.operation === 'createObject') {
                    const toInsert = operation.args instanceof Array ? operation.args[0] : operation.args
                    for (const {path, placeholder} of operation.replace || []) {
                        toInsert[path as string] = placeholders[placeholder].id
                    }

                    const { object } = await this.createObject(operation.collection, toInsert, { entityManager })
                    if (operation.placeholder) {
                        info[operation.placeholder] = { object }
                        placeholders[operation.placeholder] = object
                    }
                } else if (operation.operation === 'updateObjects') {
                    await this.updateObjects(operation.collection, operation.where, operation.updates)
                } else if (operation.operation === 'deleteObjects') {
                    await this.deleteObjects(operation.collection, operation.where)
                } else {
                    throw new Error(`Unsupported operation in batch: ${(operation as any).operation}`)
                }
            }
        })
        return { info }
    }

    async operation(name : string, ...args : any[]) {
        if (!this.initialized) {
            throw new Error('Tried to use TypeORM backend without calling StorageManager.finishInitialization() first')
        }
        const next = { process: (context : { operation : any[] }) => super.operation(context.operation[0], ...context.operation.slice(1)) }
        const middleware = new ComplexCreateMiddleware({ storageRegistry: this.registry })
        const result = await middleware.process({ operation: [name, ...args], next })
        return result
    }

    getRepositoryForCollection(collectionName : string, options? : InternalOperationOptions & { database? : string }) {
        const entityManager = (options && options.entityManager) ? options.entityManager : this.connection!
        return entityManager.getRepository(this.entitySchemas![collectionName])
    }

    _preprocessOperation(collectionName : string, options? : { database? : string }) {
        const repository = this.getRepositoryForCollection(collectionName, options)
        const collectionDefinition = this.registry.collections[collectionName]
        return { repository, collectionDefinition }
    }

    _preprocessFilteredOperation(collectionName : string, where : any, options? : { database? : string }) {
        const { repository, collectionDefinition } = this._preprocessOperation(collectionName, options)
        const convertedWhere = convertQueryWhere(where, { collectionDefinition })
        const queryBuilder = repository.createQueryBuilder(collectionName)
        const queryBuilderWithWhere = queryBuilder.where(convertedWhere.expression, convertedWhere.placeholders)
        return { repository, collectionDefinition, convertedWhere, queryBuilderWithWhere }
    }   
}

function convertQueryWhere(where : {[key : string] : any}, options : { collectionDefinition: CollectionDefinition }) : {
    expression : string,
    placeholders : {[key : string] : any}
} {
    const convertFieldName = (fieldName : string) => {
        const relationship = options.collectionDefinition.relationshipsByAlias![fieldName]
        if (!relationship) {
            return fieldName
        }

        if (isChildOfRelationship(relationship)) {
            return relationship.fieldName!
        } else {
            throw new Error(`Not supported yet to filter by this relationship: ${fieldName}`)
        }
    }

    const placeholders : {[key : string] : any} = {}
    const expressions : string[] = []
    for (const [fieldName, predicate] of Object.entries(where)) {
        const conditions = []
        if (isPlainObject(predicate)) {
            for (const [key, value] of Object.entries(predicate)) {
                if (key.charAt(0) === '$') {
                    if (key === '$eq') { // Not a standard operator, just an internal one
                        throw new Error(`Unsupported operator '${key}' for field ''`)
                    }
                    conditions.push([key, value])
                }
            }
        }
        
        if (!conditions.length) {
            conditions.push(['$eq', predicate])
        } else if (conditions.length > 1) {
            throw new Error(`Multiple operators per field in 'where' are not supported yet`)
        }

        const [operator, rhs] = conditions[0]
        if (!OPERATORS_AS_STRINGS[operator]) {
            throw new Error(`Unsupported operator '${operator}' for field ''`)
        }

        const convertedFieldName = convertFieldName(fieldName)
        placeholders[convertedFieldName] = rhs
        if (operator === '$in') {
            expressions.push(`${options.collectionDefinition.name}.${convertedFieldName} IN (:...${convertedFieldName})`)
        } else {
            expressions.push(`${options.collectionDefinition.name}.${convertedFieldName} ${OPERATORS_AS_STRINGS[operator]} :${convertedFieldName}`)
        }
    }
    return { expression: expressions.join(' AND '), placeholders }
}

function convertOrder(order : [string, 'asc' | 'desc'][], options : { collection : string }) {
    const converted : { [fieldName : string] : 'ASC' | 'DESC' } = {}
    for (const [fieldName, direction] of order) {
        converted[`${options.collection}.${fieldName}`] = direction === 'asc' ? 'ASC' : 'DESC'
    }
    return converted
}
