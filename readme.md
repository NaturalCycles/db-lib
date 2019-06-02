## @naturalcycles/db-lib

> Lowest Common Denominator API to supported Databases

[![npm](https://img.shields.io/npm/v/@naturalcycles/db-lib/latest.svg)](https://www.npmjs.com/package/@naturalcycles/db-lib)
[![](https://circleci.com/gh/NaturalCycles/db-lib.svg?style=shield&circle-token=123)](https://circleci.com/gh/NaturalCycles/db-lib)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)

# Supported databases

- [x] GCP Datastore (Firestore in Datastore mode) (datastore-lib)
- [ ] GCP Firestore (Firestore in Native mode) (firestore-lib)
- [ ] MySQL (mysql-lib)

# Features

- CommonDB, CommonDao, DBQuery
- Streaming (Node.js streams transformed into RxJS Observable streams)
- DBM / BM, validation, conversion (Joi-powered)
- Conventions
  - String `ids`
  - `created`, `updated` (unix timestamps)
  - Dates as ISO strings, e.g `2019-06-21`
  - Timestamps as unixtimestamps (seconds, not milliseconds; UTC)
  - Complex objects as JSON serialized to string (DBM), converted to object (BM)

# Concept

CommonDB is a low-level API (no high-level sugar-syntax).
CommonDao is the opposite - a high-level API (with convenience methods), built on top of CommonDB.

Concerns of CommonDB:
- Access to DB (all tables): CRUD (create, read, update, delete)
- Batch methods (cause they can be more optimal if implemented "natively")
- Querying
- Streaming

Concerns of CommonDao:
- Access to one DB Table ("kind")
- Transformation between DBM and BM, validation/conversion
- Auto-generating `id`, `created`, `updated` fields

# Packaging

- `engines.node >= 10.13`: Latest Node.js LTS
- `main: dist/index.js`: commonjs, es2018
- `types: dist/index.d.ts`: typescript types
- `/src` folder with source `*.ts` files included
