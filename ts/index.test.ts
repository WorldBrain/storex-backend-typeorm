import {
    testStorageBackend,
    StorexBackendTestContext,
} from '@worldbrain/storex/lib/index.tests'
import { TypeORMStorageBackend } from '.'

describe('TypeORM StorageBackend tests', () => {
    testStorageBackend(async (context: StorexBackendTestContext) => {
        const backend = new TypeORMStorageBackend({
            connectionOptions: { type: 'sqlite', database: ':memory:' },
        })
        context.cleanupFunction = async () => {
            if (backend.connection) {
                await backend.connection!.close()
            }
        }

        return backend
    })
})
