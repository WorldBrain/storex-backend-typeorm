import { StorageRegistry } from '@worldbrain/storex'
import { CreateObjectDissection, dissectCreateObjectOperation, convertCreateObjectDissectionToBatch, setIn } from '@worldbrain/storex/lib/utils'
// import { CollectionDefinition } from 'storex/types'
import * as backend from '@worldbrain/storex/lib/types/backend'
import { IndexDefinition, CollectionField, CollectionDefinition } from '@worldbrain/storex/lib/types';
import { StorageBackendFeatureSupport } from '@worldbrain/storex/lib/types/backend-features';
import { UnimplementedError, InvalidOptionsError } from '@worldbrain/storex/lib/types/errors';

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
    }

    private initialized = false

    constructor(options? : {}) {
        super()
    }

    configure({ registry }: { registry: StorageRegistry }) {
        super.configure({ registry })
        registry.once('initialized', this._onRegistryInitialized)
    }

    _onRegistryInitialized = () => {
        this.initialized = true
    }

    async migrate(options : { database? : string } = {}) {
    }

    async cleanup(): Promise<any> {

    }

    async createObject(collection: string, object : any, options: backend.CreateSingleOptions = {}): Promise<backend.CreateSingleResult> {
        return {}
    }

    async findObjects<T>(collection: string, where : any, findOpts: backend.FindManyOptions = {}): Promise<Array<T>> {
        return []
    }

    async updateObjects(collection: string, where : any, updates : any, options: backend.UpdateManyOptions = {}): Promise<backend.UpdateManyResult> {
    }

    async deleteObjects(collection: string, where : any, options: backend.DeleteManyOptions = {}): Promise<backend.DeleteManyResult> {
        
    }

    async countObjects(collection: string, where : any) : Promise<number> {
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
}
