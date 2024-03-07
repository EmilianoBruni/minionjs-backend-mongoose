# minionjs-backend-mongoose - Changelog

_A Mongoose Backend written in Typescript/ES6 for minion.js, a high performance job queue for Node.js_

## [0.1.3] 2024-03-07

### Added

- Add documentation

### Fixed

- Fix errors when connection has gone away because end() hash been called.
- Dequeue algorith doesn't works well if document are never enqueued. Fixed.

## [0.1.2] 2024-03-06

### Fixed

- Fix some errors in connection and disconnection
- Fix dependences

## [0.1.1] 2024-03-04

### Fixed

- Fix bug when backend creates the connection
- Fix documentation on connection

## [0.1.0] 2024-03-02

### Added

- First public release