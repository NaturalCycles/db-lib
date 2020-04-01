## @naturalcycles/db-lib

> Lowest Common Denominator API to supported Databases

[![npm](https://img.shields.io/npm/v/@naturalcycles/db-lib/latest.svg)](https://www.npmjs.com/package/@naturalcycles/db-lib)
[![](https://circleci.com/gh/NaturalCycles/db-lib.svg?style=shield&circle-token=123)](https://circleci.com/gh/NaturalCycles/db-lib)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)

# Supported databases

- [x] InMemoryDB
- [x] SimpleFileDB (persistence in local json files)
- [x] [datastore-lib](https://github.com/NaturalCycles/datastore-lib) (GCP Datastore, or Firestore
      in Datastore mode)
- [x] [firestore-lib](https://github.com/NaturalCycles/firestore-lib) (Firestore in Native mode)
- [x] [mysql-lib](https://github.com/NaturalCycles/mysql-lib) (MySQL)
- [x] [redis-lib](https://github.com/NaturalCycles/redis-lib) (Redis)
- [x] [mongo-lib](https://github.com/NaturalCycles/mongo-lib) (MongoDB)
- [x] [airtable-lib](https://github.com/NaturalCycles/airtable-lib) (Airtable)

# Features

- CommonDB, CommonDao, DBQuery
- Streaming (Node.js streams with backpressure)
- DBM / BM, validation, conversion (Joi-powered)
- Conventions
  - String `ids`
  - `created`, `updated` (unix timestamps)
  - Dates as ISO strings, e.g `2019-06-21`
  - Timestamps as unixtimestamps (seconds, not milliseconds; UTC)
  - Complex objects as JSON serialized to string (DBM), converted to object (BM)

# Concept

CommonDB is a low-level API (no high-level sugar-syntax). CommonDao is the opposite - a high-level
API (with convenience methods), built on top of CommonDB.

Concerns of CommonDB:

- Access to DB (all tables): CRUD (create, read, update, delete)
- Batch methods (cause they can be more optimal if implemented "natively")
- Querying
- Streaming

Concerns of CommonDao:

- Access to one DB Table ("kind")
- Transformation between DBM and BM, validation/conversion
- Auto-generating `id`, `created`, `updated` fields
- Anonymization hook to be able to plug your implementation (privacy by design)

# DEBUG namespaces

- `nc:db-lib:commondao`
- `nc:db-lib:inmemorydb`

# Exports

- `/` root
- `/adapter/cachedb`
- `/adapter/noop`
- `/adapter/simplefile`
- `/testing`
  - dbTest
  - daoTest
  - Test models, utils, etc
- `/validation`
  - Joi validation schemas for DBQuery, CommonDBOptions, CommonSchema, etc

# Packaging

- `engines.node >= LTS`
- `main: dist/index.js`: commonjs, es2019
- `types: dist/index.d.ts`: typescript types
- `/src` folder with source `*.ts` files included
