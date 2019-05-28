import isPlainObject from 'lodash/isPlainObject'
import { StorageRegistry } from '@worldbrain/storex'
import { CreateObjectDissection, dissectCreateObjectOperation, convertCreateObjectDissectionToBatch, setIn } from '@worldbrain/storex/lib/utils'
// import { CollectionDefinition } from 'storex/types'
import * as backend from '@worldbrain/storex/lib/types/backend'
import { IndexDefinition, CollectionField, CollectionDefinition } from '@worldbrain/storex/lib/types';
import { StorageBackendFeatureSupport } from '@worldbrain/storex/lib/types/backend-features';
import { UnimplementedError, InvalidOptionsError } from '@worldbrain/storex/lib/types/errors';
import * as typeorm from 'typeorm'
import { Connection, ConnectionOptions, createConnection, EntitySchema } from 'typeorm';
import { collectionsToEntitySchemas } from './entities';
import { cleanOptionalFieldsForRead, ObjectCleaner, makeCleanerChain, makeCleanBooleanFieldsForRead, cleanRelationshipFieldsForWrite, cleanRelationshipFieldsForRead } from './utils';

const OPERATORS = {
    $lt: typeorm.LessThan,
    $lte: typeorm.LessThanOrEqual,
    $gt: typeorm.MoreThan,
    $gte: typeorm.MoreThanOrEqual,
    $ne: typeorm.Not,
}
const OPERATORS_AS_STRINGS = {
    $lt: '<',
    $lte: '<=',
    $gt: '>',
    $gte: '>=',
    $ne: '!=',
    $eq: '=',
    $in: 'IN'
}

export interface IndexedDbImplementation {
    factory: IDBFactory
    range: new () => IDBKeyRange
}

export class TypeORMStorageBackend extends backend.StorageBackend {
    features: StorageBackendFeatureSupport = {
        count: false,
        createWithRelationships: false,
        fullTextSearch: false,
        executeBatch: false,
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

    async createObject(collection : string, object : any, options: backend.CreateSingleOptions = {}): Promise<backend.CreateSingleResult> {
        const { repository, collectionDefinition } = this._preprocessOperation(collection, options)
        const cleanedObject = this.writeObjectCleaner(object, { collectionDefinition })
        const savedObject = await repository.save(cleanedObject)
        return { object: savedObject }
    }

    async findObjects<T>(collection : string, where : any, options: backend.FindManyOptions = {}): Promise<Array<T>> {
        const { collectionDefinition, queryBuilderWithWhere } = this._preprocessFilteredOperation(collection, where, options)
        const objects = await queryBuilderWithWhere
            .orderBy(convertOrder(options.order || [], { collection }))
            .take(options.limit)
            .getMany()
        
        return objects.map(object => this.readObjectCleaner(object, { collectionDefinition }))
    }

    async updateObjects(collection : string, where : any, updates : any, options : backend.UpdateManyOptions = {}): Promise<backend.UpdateManyResult> {
        const { collectionDefinition, queryBuilderWithWhere } = this._preprocessFilteredOperation(collection, where, options)
        const convertedUpdates = updates
        await queryBuilderWithWhere.update(convertedUpdates).execute()
    }

    async deleteObjects(collection : string, where : any, options : backend.DeleteManyOptions = {}): Promise<backend.DeleteManyResult> {
        const { collectionDefinition, queryBuilderWithWhere } = this._preprocessFilteredOperation(collection, where, options)
        await queryBuilderWithWhere.delete().execute()
    }

    async countObjects(collection : string, where : any, options : backend.CountOptions = {}) : Promise<number> {
        const { collectionDefinition, queryBuilderWithWhere } = this._preprocessFilteredOperation(collection, where, options)
        return -1
    }

    async executeBatch(batch : backend.OperationBatch) {
        if (!batch.length) {
            return { info: {} }
        }

        return { info: null }
    }

    async transaction(options : { collections: string[] }, body : Function) {
        const executeBody = async () => {
            return body({ transactionOperation: (name : string, ...args : any[]) => {
                return this.operation(name, ...args)
            } })
        }
    }

    async operation(name : string, ...args : any[]) {
        if (!this.initialized) {
            throw new Error('Tried to use TypeORM backend without calling StorageManager.finishInitialization() first')
        }
        return await super.operation(name, ...args)
    }

    getRepositoryForCollection(collectionName : string, options? : { database? : string }) {
        return this.connection!.getRepository(this.entitySchemas![collectionName])
    }

    _preprocessOperation(collectionName : string, options? : { database? : string }) {
        const repository = this.getRepositoryForCollection(collectionName, options)
        const collectionDefinition = this.registry.collections[collectionName]
        return { repository, collectionDefinition }
    }

    _preprocessFilteredOperation(collectionName : string, where : any, options? : { database? : string }) {
        const { repository, collectionDefinition } = this._preprocessOperation(collectionName, options)
        const convertedWhere = convertQueryWhere(where, { tableName: collectionName })
        const queryBuilder = repository.createQueryBuilder(collectionName)
        const queryBuilderWithWhere = queryBuilder.where(convertedWhere.expression, convertedWhere.placeholders)
        return { repository, collectionDefinition, convertedWhere, queryBuilderWithWhere }
    }   
}

function convertQueryWhere(where : {[key : string] : any}, options : { tableName : string }) : {
    expression : string,
    placeholders : {[key : string] : any}
} {
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

        placeholders[fieldName] = rhs
        if (operator === '$in') {
            expressions.push(`${options.tableName}.${fieldName} IN (:...${fieldName})`)
        } else {
            expressions.push(`${options.tableName}.${fieldName} ${OPERATORS_AS_STRINGS[operator]} :${fieldName}`)
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
