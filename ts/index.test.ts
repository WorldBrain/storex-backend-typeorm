import { createConnection } from 'typeorm'
import { testStorageBackend, StorexBackendTestContext } from "@worldbrain/storex/lib/index.tests"
import { TypeORMStorageBackend } from "."

describe('TypeORM StorageBackend tests', () => {
    testStorageBackend(async (context : StorexBackendTestContext) => {
        const connection = await createConnection({ type: 'sqlite', database: ':memory:' });
        context.cleanupFunction = async () => {
            connection.close()
        }

        const backend = new TypeORMStorageBackend({ connection })
        return backend
    })
})
