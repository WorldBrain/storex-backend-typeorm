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
import { cleanOptionalFieldsForRead, ObjectCleaner, makeCleanerChain, makeCleanBooleanFieldsForRead } from './utils';

const OPERATORS = {
    $lt: typeorm.LessThan,
    $lte: typeorm.LessThanOrEqual,
    $gt: typeorm.MoreThan,
    $gte: typeorm.MoreThanOrEqual,
    $ne: typeorm.Not,
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
            cleanOptionalFieldsForRead
        ])
        this.writeObjectCleaner = makeCleanerChain([

        ])
    }

    async migrate(options : { database? : string } = {}) {
        await this.connection!.synchronize()
    }

    async cleanup(): Promise<any> {

    }

    async createObject(collection : string, object : any, options: backend.CreateSingleOptions = {}): Promise<backend.CreateSingleResult> {
        const repository = this.getRepositoryForCollection(collection, options)
        const savedObject = await repository.save(object)
        return { object: savedObject }
    }

    async findObjects<T>(collection : string, where : any, options: backend.FindManyOptions = {}): Promise<Array<T>> {
        const { repository, collectionDefinition, convertedWhere } = this._preprocessFilteredOperation(collection, where, options)
        const objects = await repository.find({
            where: convertedWhere,
            order: convertOrder(options.order || []),
            take: options.limit,
        })
        return objects.map(object => this.readObjectCleaner(object, { collectionDefinition }))
    }

    async updateObjects(collection : string, where : any, updates : any, options : backend.UpdateManyOptions = {}): Promise<backend.UpdateManyResult> {
        const { repository, collectionDefinition, convertedWhere } = this._preprocessFilteredOperation(collection, where, options)
    }

    async deleteObjects(collection : string, where : any, options : backend.DeleteManyOptions = {}): Promise<backend.DeleteManyResult> {
        const { repository, collectionDefinition, convertedWhere } = this._preprocessFilteredOperation(collection, where, options)
    }

    async countObjects(collection : string, where : any, options : backend.CountOptions = {}) : Promise<number> {
        const { repository, collectionDefinition, convertedWhere } = this._preprocessFilteredOperation(collection, where, options)
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

    _preprocessFilteredOperation(collectionName : string, where : any, options? : { database? : string }) {
        const repository = this.getRepositoryForCollection(collectionName, options)
        const collectionDefinition = this.registry.collections[collectionName]
        const convertedWhere = convertQueryWhere(where)
        return { repository, collectionDefinition, convertedWhere }
    }   
}

function convertQueryWhere(where : any) {
    for (const [fieldName, predicate] of Object.entries(where)) {
        if (isPlainObject(predicate)) {
            for (const [key, value] of Object.entries(predicate)) {
                if (key.charAt(0) === '$') {
                    where[fieldName] = OPERATORS[key](value)
                }
            }
        }
    }

    return where
}

function convertOrder(order : [string, 'asc' | 'desc'][]) {
    const converted : { [fieldName : string] : 'ASC' | 'DESC' } = {}
    for (const [fieldName, direction] of order) {
        converted[fieldName] = direction === 'asc' ? 'ASC' : 'DESC'
    }
    return converted
}
