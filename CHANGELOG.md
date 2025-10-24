## [1.0.1-next.1](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0...v1.0.1-next.1) (2025-10-24)


### Bug Fixes

* potential issue with re-running jobs ([2242de0](https://github.com/Openpanel-dev/groupmq/commit/2242de0f70e26d1f98ab5491412a755b83269106))

## 1.0.0 (2025-10-09)


### Features

* (sort of) stable v1.0.0 release ([1d40049](https://github.com/Openpanel-dev/groupmq/commit/1d4004998d2b41a1efca4e296dc8636790ce7eca))
* add clean method ([8cf533a](https://github.com/Openpanel-dev/groupmq/commit/8cf533a51f780117873470676437fae7deb1ea66))
* add name property to the queue ([7539fa4](https://github.com/Openpanel-dev/groupmq/commit/7539fa41519aea5ec75df86ded53c203521ac5ff))
* add stalled jobs and better close/disconnect handling ([652118b](https://github.com/Openpanel-dev/groupmq/commit/652118bbfb4a40f13815f6ed9ec89774de74b6a1))
* better ordering options that actually works ([6f5e999](https://github.com/Openpanel-dev/groupmq/commit/6f5e9991079b302b076c64e288a7619e7da3ad25))
* mvp ([86639dd](https://github.com/Openpanel-dev/groupmq/commit/86639ddd34b293413f9a3d1ba77da7dc83e87973))
* promote, retry and remove ([424533b](https://github.com/Openpanel-dev/groupmq/commit/424533b6cb8a78bc5fde6db910391c4967d83b33))
* update job data ([531013b](https://github.com/Openpanel-dev/groupmq/commit/531013b622356e8a3a8c629dcb46738aba619105))


### Bug Fixes

* actually fix ordering again ([e48ff41](https://github.com/Openpanel-dev/groupmq/commit/e48ff41a664bc59710c8f297b809aa5ed89b2afe))
* async fifo queue ([c94bb8c](https://github.com/Openpanel-dev/groupmq/commit/c94bb8c7769fa3ba9e26d607e827fa7ff9227666))
* better cleaning ([434f4b1](https://github.com/Openpanel-dev/groupmq/commit/434f4b138caf62a1c40f383d581b4cb35901eb12))
* better cpu usage for multiple workers, concurrency support ([f59a4f5](https://github.com/Openpanel-dev/groupmq/commit/f59a4f587dd5a1868917d76187d0f04dadf314c5))
* better shutdown and more ordering options ([5dff7ae](https://github.com/Openpanel-dev/groupmq/commit/5dff7ae6cf1841bf62d7a96cb2f2e5fea8084919))
* cjs import ([d1203e5](https://github.com/Openpanel-dev/groupmq/commit/d1203e5bfea0df9a6e5768d01d99b886439f0499))
* concurrency ([f4ed7be](https://github.com/Openpanel-dev/groupmq/commit/f4ed7bee1499946bc424f58fb33cc0bb6ced549f))
* concurrency and lua script ([9b8abe5](https://github.com/Openpanel-dev/groupmq/commit/9b8abe5b1d3bac2ba1865684b14c11d0b9e33ba5))
* concurrency issue ([e9f70bb](https://github.com/Openpanel-dev/groupmq/commit/e9f70bbdc24107da62ce8ce0ea86b08b209c5c96))
* correct process time (mainly for bullboard) ([68e229c](https://github.com/Openpanel-dev/groupmq/commit/68e229cfcd5a57166e8dc355e975c158553bbb25))
* correct the exports in pkgjson ([d0aafa4](https://github.com/Openpanel-dev/groupmq/commit/d0aafa41945e709a585a0cc9352cdb22174eddc3))
* ensure orderMs is a number (fallback to timestamp) ([42f5e9d](https://github.com/Openpanel-dev/groupmq/commit/42f5e9da765817f04b12042a3e73d7c66342ebdb))
* errors in bullboard adapater ([f017602](https://github.com/Openpanel-dev/groupmq/commit/f017602019ffdd028b535e7b2885b2705ef7fe5d))
* flaky test (use performance.now() instead) ([ea466b9](https://github.com/Openpanel-dev/groupmq/commit/ea466b9897bfc5fd4c3120e40a5daa8584399f99))
* handle sub 1500ms cron jobs (new setting schedulerLockTtlMs) ([b40f3fe](https://github.com/Openpanel-dev/groupmq/commit/b40f3fe3b5c7565ffd434ee155187f70d877fdbb))
* improve processOne ([3770f02](https://github.com/Openpanel-dev/groupmq/commit/3770f0221bf205e5f4c7a005c0f419184a755434))
* improve test speed and flakeyness ([7110899](https://github.com/Openpanel-dev/groupmq/commit/7110899f4e7be24e8247d74b7b29a156a1063c61))
* improve tests, queue and worker ([87665d9](https://github.com/Openpanel-dev/groupmq/commit/87665d9b7c9cc14290a245ecc532a5d120a64bb2))
* logger interface (support pino and winston) ([69917d4](https://github.com/Openpanel-dev/groupmq/commit/69917d4912a5b81881c5e31777e9e0d4987d554f))
* logging issues ([2f6e7aa](https://github.com/Openpanel-dev/groupmq/commit/2f6e7aaf3dc2710e37c38113c3a2bbc7fcd06fa8))
* make completion atomic (and some cleanup) ([35b10f4](https://github.com/Openpanel-dev/groupmq/commit/35b10f4488705b8b7b898884edc27298a6103ab0))
* remove get delay ms ([c714792](https://github.com/Openpanel-dev/groupmq/commit/c7147920a52733eecb8c6a044fa36b8e01812336))
* replace seq counter with epoch counter ([ba38a76](https://github.com/Openpanel-dev/groupmq/commit/ba38a762593625af950cf86335aa293dba1e0856))
* retry release ([9559a40](https://github.com/Openpanel-dev/groupmq/commit/9559a40143ad4d6745bfd369dfd82d886b4ed6bb))
* return job from eval instead of fetching job after add ([86de438](https://github.com/Openpanel-dev/groupmq/commit/86de438ca1993aeaa118ad7e280f0037064345f6))
* tests ([0f66afb](https://github.com/Openpanel-dev/groupmq/commit/0f66afb7cf1a6d076832348c87a349721ab88b88))
* timings ([9fef5f6](https://github.com/Openpanel-dev/groupmq/commit/9fef5f66f2a6521abec06f5b5458867924b50cf7))
* use reserve atomic to avoid workers grabbing same jobs ([2ef73f3](https://github.com/Openpanel-dev/groupmq/commit/2ef73f3a2a80ec51d869b913e60e02ff5f120567))
* workflow issues ([c1f2489](https://github.com/Openpanel-dev/groupmq/commit/c1f2489fe6e38c24af754a1ebad8b81012596e26))

## [1.0.0-next.19](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.18...v1.0.0-next.19) (2025-10-09)


### Bug Fixes

* correct process time (mainly for bullboard) ([68e229c](https://github.com/Openpanel-dev/groupmq/commit/68e229cfcd5a57166e8dc355e975c158553bbb25))

## [1.0.0-next.18](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.17...v1.0.0-next.18) (2025-10-08)


### Features

* add stalled jobs and better close/disconnect handling ([652118b](https://github.com/Openpanel-dev/groupmq/commit/652118bbfb4a40f13815f6ed9ec89774de74b6a1))


### Bug Fixes

* make completion atomic (and some cleanup) ([35b10f4](https://github.com/Openpanel-dev/groupmq/commit/35b10f4488705b8b7b898884edc27298a6103ab0))
* return job from eval instead of fetching job after add ([86de438](https://github.com/Openpanel-dev/groupmq/commit/86de438ca1993aeaa118ad7e280f0037064345f6))

## [1.0.0-next.17](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.16...v1.0.0-next.17) (2025-10-07)


### Bug Fixes

* actually fix ordering again ([e48ff41](https://github.com/Openpanel-dev/groupmq/commit/e48ff41a664bc59710c8f297b809aa5ed89b2afe))

## [1.0.0-next.16](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.15...v1.0.0-next.16) (2025-10-07)


### Bug Fixes

* better shutdown and more ordering options ([5dff7ae](https://github.com/Openpanel-dev/groupmq/commit/5dff7ae6cf1841bf62d7a96cb2f2e5fea8084919))
* ensure orderMs is a number (fallback to timestamp) ([42f5e9d](https://github.com/Openpanel-dev/groupmq/commit/42f5e9da765817f04b12042a3e73d7c66342ebdb))

## [1.0.0-next.15](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.14...v1.0.0-next.15) (2025-10-06)


### Features

* better ordering options that actually works ([6f5e999](https://github.com/Openpanel-dev/groupmq/commit/6f5e9991079b302b076c64e288a7619e7da3ad25))

## [1.0.0-next.14](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.13...v1.0.0-next.14) (2025-10-05)


### Bug Fixes

* use reserve atomic to avoid workers grabbing same jobs ([2ef73f3](https://github.com/Openpanel-dev/groupmq/commit/2ef73f3a2a80ec51d869b913e60e02ff5f120567))

## [1.0.0-next.13](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.12...v1.0.0-next.13) (2025-10-04)


### Bug Fixes

* logging issues ([2f6e7aa](https://github.com/Openpanel-dev/groupmq/commit/2f6e7aaf3dc2710e37c38113c3a2bbc7fcd06fa8))

## [1.0.0-next.12](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.11...v1.0.0-next.12) (2025-10-04)


### Bug Fixes

* handle sub 1500ms cron jobs (new setting schedulerLockTtlMs) ([b40f3fe](https://github.com/Openpanel-dev/groupmq/commit/b40f3fe3b5c7565ffd434ee155187f70d877fdbb))
* improve test speed and flakeyness ([7110899](https://github.com/Openpanel-dev/groupmq/commit/7110899f4e7be24e8247d74b7b29a156a1063c61))

## [1.0.0-next.11](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.10...v1.0.0-next.11) (2025-10-03)


### Bug Fixes

* async fifo queue ([c94bb8c](https://github.com/Openpanel-dev/groupmq/commit/c94bb8c7769fa3ba9e26d607e827fa7ff9227666))
* concurrency ([f4ed7be](https://github.com/Openpanel-dev/groupmq/commit/f4ed7bee1499946bc424f58fb33cc0bb6ced549f))
* improve processOne ([3770f02](https://github.com/Openpanel-dev/groupmq/commit/3770f0221bf205e5f4c7a005c0f419184a755434))
* remove get delay ms ([c714792](https://github.com/Openpanel-dev/groupmq/commit/c7147920a52733eecb8c6a044fa36b8e01812336))

## [1.0.0-next.10](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.9...v1.0.0-next.10) (2025-10-03)


### Bug Fixes

* errors in bullboard adapater ([f017602](https://github.com/Openpanel-dev/groupmq/commit/f017602019ffdd028b535e7b2885b2705ef7fe5d))

## [1.0.0-next.9](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.8...v1.0.0-next.9) (2025-10-03)


### Bug Fixes

* flaky test (use performance.now() instead) ([ea466b9](https://github.com/Openpanel-dev/groupmq/commit/ea466b9897bfc5fd4c3120e40a5daa8584399f99))
* replace seq counter with epoch counter ([ba38a76](https://github.com/Openpanel-dev/groupmq/commit/ba38a762593625af950cf86335aa293dba1e0856))
* tests ([0f66afb](https://github.com/Openpanel-dev/groupmq/commit/0f66afb7cf1a6d076832348c87a349721ab88b88))

## [1.0.0-next.8](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.7...v1.0.0-next.8) (2025-10-02)


### Bug Fixes

* improve tests, queue and worker ([87665d9](https://github.com/Openpanel-dev/groupmq/commit/87665d9b7c9cc14290a245ecc532a5d120a64bb2))
* timings ([9fef5f6](https://github.com/Openpanel-dev/groupmq/commit/9fef5f66f2a6521abec06f5b5458867924b50cf7))

## [1.0.0-next.7](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.6...v1.0.0-next.7) (2025-10-02)


### Bug Fixes

* concurrency and lua script ([9b8abe5](https://github.com/Openpanel-dev/groupmq/commit/9b8abe5b1d3bac2ba1865684b14c11d0b9e33ba5))
* concurrency issue ([e9f70bb](https://github.com/Openpanel-dev/groupmq/commit/e9f70bbdc24107da62ce8ce0ea86b08b209c5c96))
* logger interface (support pino and winston) ([69917d4](https://github.com/Openpanel-dev/groupmq/commit/69917d4912a5b81881c5e31777e9e0d4987d554f))

## [1.0.0-next.6](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.5...v1.0.0-next.6) (2025-10-02)


### Bug Fixes

* better cleaning ([434f4b1](https://github.com/Openpanel-dev/groupmq/commit/434f4b138caf62a1c40f383d581b4cb35901eb12))
* better cpu usage for multiple workers, concurrency support ([f59a4f5](https://github.com/Openpanel-dev/groupmq/commit/f59a4f587dd5a1868917d76187d0f04dadf314c5))

## [1.0.0-next.5](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.4...v1.0.0-next.5) (2025-10-01)


### Features

* add name property to the queue ([7539fa4](https://github.com/Openpanel-dev/groupmq/commit/7539fa41519aea5ec75df86ded53c203521ac5ff))

## [1.0.0-next.4](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.3...v1.0.0-next.4) (2025-10-01)


### Bug Fixes

* cjs import ([d1203e5](https://github.com/Openpanel-dev/groupmq/commit/d1203e5bfea0df9a6e5768d01d99b886439f0499))

## [1.0.0-next.3](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.2...v1.0.0-next.3) (2025-10-01)


### Bug Fixes

* correct the exports in pkgjson ([d0aafa4](https://github.com/Openpanel-dev/groupmq/commit/d0aafa41945e709a585a0cc9352cdb22174eddc3))

## [1.0.0-next.2](https://github.com/Openpanel-dev/groupmq/compare/v1.0.0-next.1...v1.0.0-next.2) (2025-09-29)


### Features

* add clean method ([8cf533a](https://github.com/Openpanel-dev/groupmq/commit/8cf533a51f780117873470676437fae7deb1ea66))
* promote, retry and remove ([424533b](https://github.com/Openpanel-dev/groupmq/commit/424533b6cb8a78bc5fde6db910391c4967d83b33))
* update job data ([531013b](https://github.com/Openpanel-dev/groupmq/commit/531013b622356e8a3a8c629dcb46738aba619105))

## 1.0.0-next.1 (2025-09-23)


### Features

* mvp ([86639dd](https://github.com/Openpanel-dev/groupmq/commit/86639ddd34b293413f9a3d1ba77da7dc83e87973))


### Bug Fixes

* retry release ([9559a40](https://github.com/Openpanel-dev/groupmq/commit/9559a40143ad4d6745bfd369dfd82d886b4ed6bb))
* workflow issues ([c1f2489](https://github.com/Openpanel-dev/groupmq/commit/c1f2489fe6e38c24af754a1ebad8b81012596e26))

## 1.0.0-next.1 (2025-09-23)


### Features

* mvp ([86639dd](https://github.com/Openpanel-dev/groupmq/commit/86639ddd34b293413f9a3d1ba77da7dc83e87973))
