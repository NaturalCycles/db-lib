# [2.0.0](https://github.com/NaturalCycles/db-lib/compare/v1.24.1...v2.0.0) (2019-10-18)


### Features

* refactor RxJS streams into Node stream w/ backpressure ([3268150](https://github.com/NaturalCycles/db-lib/commit/3268150))


### BREAKING CHANGES

* ^^^

## [1.24.1](https://github.com/NaturalCycles/db-lib/compare/v1.24.0...v1.24.1) (2019-10-18)


### Bug Fixes

* pin @types/hapi__joi ([14cee06](https://github.com/NaturalCycles/db-lib/commit/14cee06))

# [1.24.0](https://github.com/NaturalCycles/db-lib/compare/v1.23.2...v1.24.0) (2019-09-30)


### Features

* DBQuery.offset ([2995253](https://github.com/NaturalCycles/db-lib/commit/2995253))

## [1.23.2](https://github.com/NaturalCycles/db-lib/compare/v1.23.1...v1.23.2) (2019-09-21)


### Bug Fixes

* truncate long getByIds, saveBatch logs ([6826ffc](https://github.com/NaturalCycles/db-lib/commit/6826ffc))

## [1.23.1](https://github.com/NaturalCycles/db-lib/compare/v1.23.0...v1.23.1) (2019-09-21)


### Bug Fixes

* change DBM/BM order, again ([ff388d2](https://github.com/NaturalCycles/db-lib/commit/ff388d2))

# [1.23.0](https://github.com/NaturalCycles/db-lib/compare/v1.22.0...v1.23.0) (2019-09-21)


### Features

* CommonDBTestOptions.eventualConsistencyDelay ([83839dc](https://github.com/NaturalCycles/db-lib/commit/83839dc))

# [1.22.0](https://github.com/NaturalCycles/db-lib/compare/v1.21.0...v1.22.0) (2019-09-21)


### Features

* refactor types, now order is DBM, BM, TM ([a52d236](https://github.com/NaturalCycles/db-lib/commit/a52d236))

# [1.21.0](https://github.com/NaturalCycles/db-lib/compare/v1.20.1...v1.21.0) (2019-09-21)


### Features

* RunnableDBQuery ([b045e89](https://github.com/NaturalCycles/db-lib/commit/b045e89))

## [1.20.1](https://github.com/NaturalCycles/db-lib/compare/v1.20.0...v1.20.1) (2019-09-20)


### Bug Fixes

* remove test-lib dependency ([97bf9d3](https://github.com/NaturalCycles/db-lib/commit/97bf9d3))

# [1.20.0](https://github.com/NaturalCycles/db-lib/compare/v1.19.0...v1.20.0) (2019-09-19)


### Features

* CommonDao.getAll() ([3f39b1c](https://github.com/NaturalCycles/db-lib/commit/3f39b1c))

# [1.19.0](https://github.com/NaturalCycles/db-lib/compare/v1.18.0...v1.19.0) (2019-09-19)


### Features

* refactor saved/unsaved models ([308d091](https://github.com/NaturalCycles/db-lib/commit/308d091))

# [1.18.0](https://github.com/NaturalCycles/db-lib/compare/v1.17.1...v1.18.0) (2019-09-19)


### Features

* CommonDao.getByIdAsTM() ([b3f4e2b](https://github.com/NaturalCycles/db-lib/commit/b3f4e2b))

## [1.17.1](https://github.com/NaturalCycles/db-lib/compare/v1.17.0...v1.17.1) (2019-09-19)


### Bug Fixes

* CommonDao.cfg public ([810a6fd](https://github.com/NaturalCycles/db-lib/commit/810a6fd))

# [1.17.0](https://github.com/NaturalCycles/db-lib/compare/v1.16.0...v1.17.0) (2019-09-19)


### Features

* CommonDaoCfg.excludeFromIndexes ([29d03b7](https://github.com/NaturalCycles/db-lib/commit/29d03b7))

# [1.16.0](https://github.com/NaturalCycles/db-lib/compare/v1.15.0...v1.16.0) (2019-09-19)


### Features

* CommonDao revert runQuery, provide runQueryExtended ([008a7bc](https://github.com/NaturalCycles/db-lib/commit/008a7bc))

# [1.15.0](https://github.com/NaturalCycles/db-lib/compare/v1.14.0...v1.15.0) (2019-09-19)


### Features

* runQuery to return RunQueryResult ([fd2b003](https://github.com/NaturalCycles/db-lib/commit/fd2b003))

# [1.14.0](https://github.com/NaturalCycles/db-lib/compare/v1.13.1...v1.14.0) (2019-09-19)


### Features

* DBQuery.startCursor(), endCursor() ([0090631](https://github.com/NaturalCycles/db-lib/commit/0090631))

## [1.13.1](https://github.com/NaturalCycles/db-lib/compare/v1.13.0...v1.13.1) (2019-09-18)


### Bug Fixes

* daoTest ([808e4a7](https://github.com/NaturalCycles/db-lib/commit/808e4a7))

# [1.13.0](https://github.com/NaturalCycles/db-lib/compare/v1.12.0...v1.13.0) (2019-09-18)


### Features

* CommonDBTestOptions ([fd82bab](https://github.com/NaturalCycles/db-lib/commit/fd82bab))

# [1.12.0](https://github.com/NaturalCycles/db-lib/compare/v1.11.0...v1.12.0) (2019-09-18)


### Features

* include db-dev-lib tests here ([3d324c9](https://github.com/NaturalCycles/db-lib/commit/3d324c9))

# [1.11.0](https://github.com/NaturalCycles/db-lib/compare/v1.10.3...v1.11.0) (2019-09-18)


### Features

* anonymize hook ([3bf5690](https://github.com/NaturalCycles/db-lib/commit/3bf5690))
* simplify Saved/Unsaved schemas. Add TM ([4670693](https://github.com/NaturalCycles/db-lib/commit/4670693))

## [1.10.3](https://github.com/NaturalCycles/db-lib/compare/v1.10.2...v1.10.3) (2019-09-15)


### Bug Fixes

* id to use stringSchema instead of idSchema ([bd0047a](https://github.com/NaturalCycles/db-lib/commit/bd0047a))

## [1.10.2](https://github.com/NaturalCycles/db-lib/compare/v1.10.1...v1.10.2) (2019-08-23)


### Bug Fixes

* tests ([2934c74](https://github.com/NaturalCycles/db-lib/commit/2934c74))

## [1.10.1](https://github.com/NaturalCycles/db-lib/compare/v1.10.0...v1.10.1) (2019-08-23)


### Bug Fixes

* inMemoryDB ([9023c84](https://github.com/NaturalCycles/db-lib/commit/9023c84))

# [1.10.0](https://github.com/NaturalCycles/db-lib/compare/v1.9.1...v1.10.0) (2019-08-23)


### Features

* deleteByIds returns number; 'in' query operator ([15e114a](https://github.com/NaturalCycles/db-lib/commit/15e114a))

## [1.9.1](https://github.com/NaturalCycles/db-lib/compare/v1.9.0...v1.9.1) (2019-08-20)


### Bug Fixes

* test ([34c7dc0](https://github.com/NaturalCycles/db-lib/commit/34c7dc0))

# [1.9.0](https://github.com/NaturalCycles/db-lib/compare/v1.8.1...v1.9.0) (2019-08-20)


### Features

* CommonDB.saveBatch returns void ([a3ce3d4](https://github.com/NaturalCycles/db-lib/commit/a3ce3d4))

## [1.8.1](https://github.com/NaturalCycles/db-lib/compare/v1.8.0...v1.8.1) (2019-08-18)


### Bug Fixes

* test passing ([e232db5](https://github.com/NaturalCycles/db-lib/commit/e232db5))

# [1.8.0](https://github.com/NaturalCycles/db-lib/compare/v1.7.2...v1.8.0) (2019-08-18)


### Bug Fixes

* log table ([270151c](https://github.com/NaturalCycles/db-lib/commit/270151c))


### Features

* CacheDB; deleteBy > deleteByQuery ([7e75aa8](https://github.com/NaturalCycles/db-lib/commit/7e75aa8))
* SimpleFileDB, NoOpDB ([cdd6d53](https://github.com/NaturalCycles/db-lib/commit/cdd6d53))

## [1.7.2](https://github.com/NaturalCycles/db-lib/compare/v1.7.1...v1.7.2) (2019-08-18)


### Bug Fixes

* logging issue, caching issue ([649aa3a](https://github.com/NaturalCycles/db-lib/commit/649aa3a))

## [1.7.1](https://github.com/NaturalCycles/db-lib/compare/v1.7.0...v1.7.1) (2019-08-18)


### Bug Fixes

* fix ([1881a32](https://github.com/NaturalCycles/db-lib/commit/1881a32))

# [1.7.0](https://github.com/NaturalCycles/db-lib/compare/v1.6.0...v1.7.0) (2019-08-18)


### Features

* InMemoryCacheDB log, global settings ([995802f](https://github.com/NaturalCycles/db-lib/commit/995802f))

# [1.6.0](https://github.com/NaturalCycles/db-lib/compare/v1.5.4...v1.6.0) (2019-08-18)


### Features

* InMemoryCacheDB ([1b77f8d](https://github.com/NaturalCycles/db-lib/commit/1b77f8d))

## [1.5.4](https://github.com/NaturalCycles/db-lib/compare/v1.5.3...v1.5.4) (2019-08-17)


### Bug Fixes

* change debug namespace to nc:* ([ff801d5](https://github.com/NaturalCycles/db-lib/commit/ff801d5))

## [1.5.3](https://github.com/NaturalCycles/db-lib/compare/v1.5.2...v1.5.3) (2019-08-11)


### Bug Fixes

* idSchema ([57606cb](https://github.com/NaturalCycles/db-lib/commit/57606cb))

## [1.5.2](https://github.com/NaturalCycles/db-lib/compare/v1.5.1...v1.5.2) (2019-08-11)


### Bug Fixes

* tests ([ece1544](https://github.com/NaturalCycles/db-lib/commit/ece1544))

## [1.5.1](https://github.com/NaturalCycles/db-lib/compare/v1.5.0...v1.5.1) (2019-08-11)


### Bug Fixes

* fixes ([46c8f12](https://github.com/NaturalCycles/db-lib/commit/46c8f12))

# [1.5.0](https://github.com/NaturalCycles/db-lib/compare/v1.4.2...v1.5.0) (2019-08-10)


### Features

* Unsaved ([2c477be](https://github.com/NaturalCycles/db-lib/commit/2c477be))

## [1.4.2](https://github.com/NaturalCycles/db-lib/compare/v1.4.1...v1.4.2) (2019-08-10)


### Bug Fixes

* better log formatting ([aaaeb66](https://github.com/NaturalCycles/db-lib/commit/aaaeb66))

## [1.4.1](https://github.com/NaturalCycles/db-lib/compare/v1.4.0...v1.4.1) (2019-08-10)


### Bug Fixes

* log spacing ([e103132](https://github.com/NaturalCycles/db-lib/commit/e103132))

# [1.4.0](https://github.com/NaturalCycles/db-lib/compare/v1.3.0...v1.4.0) (2019-08-10)


### Features

* logging! ([f48eda7](https://github.com/NaturalCycles/db-lib/commit/f48eda7))

# [1.3.0](https://github.com/NaturalCycles/db-lib/compare/v1.2.1...v1.3.0) (2019-07-28)


### Features

* use db-dev-lib, fix in-mem .select([]) ([d79ec6c](https://github.com/NaturalCycles/db-lib/commit/d79ec6c))

## [1.2.1](https://github.com/NaturalCycles/db-lib/compare/v1.2.0...v1.2.1) (2019-06-03)


### Bug Fixes

* createQuery() ([18be3a7](https://github.com/NaturalCycles/db-lib/commit/18be3a7))

# [1.2.0](https://github.com/NaturalCycles/db-lib/compare/v1.1.0...v1.2.0) (2019-06-02)


### Features

* extract assignIdCreatedUpdated ([016aa54](https://github.com/NaturalCycles/db-lib/commit/016aa54))

# [1.1.0](https://github.com/NaturalCycles/db-lib/compare/v1.0.0...v1.1.0) (2019-06-02)


### Features

* progress ([4339ce4](https://github.com/NaturalCycles/db-lib/commit/4339ce4))

# 1.0.0 (2019-06-02)


### Features

* first version ([8d0a101](https://github.com/NaturalCycles/db-lib/commit/8d0a101))
* init project by create-module ([dec84c4](https://github.com/NaturalCycles/db-lib/commit/dec84c4))
