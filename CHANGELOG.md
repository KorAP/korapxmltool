# Changelog

## [Unreleased]

### Fixed

- Plain NOW export now opens ZIP input with `java.util.zip.ZipFile` in streaming mode instead of Apache Commons `ZipFile`, removing the multi-minute startup delay on very large archives with huge entry counts and allowing extraction to begin almost immediately
- Plain NOW export startup and progress diagnostics now log ZIP open time and first-output timing more explicitly, making it easier to distinguish ZIP indexing overhead from actual extraction work
- Plain Word2Vec export now uses the same archive-order streaming ZIP path as plain NOW output, including the faster `java.util.zip.ZipFile` opener for large archives with many entries
- Plain CoNLL-U export now also uses the archive-order streaming ZIP path when exactly one base ZIP is given, while multi-ZIP and foundry-paired CoNLL-U input stays on the ordered pipeline
- CoreNLP now uses the the base tokenization and sentence boundaries instead of its own, preventing morpho/constituency drifts (#38).

## [v3.3.0] - 2026-03-26

### Fixed

- Invocation summary (call options, environment) is no longer printed to stderr; it is written only to the log file
- Shell wrapper auto-detection message (memory/workload) is no longer echoed to stderr; passed via environment variable and included in the log file instead
- Fixed `java.util.logging.ErrorManager` NPE on JVM shutdown caused by `LOGGER` calls inside shutdown hooks executing after `LogManager.reset()` had already closed file handlers
- For annotation and krill output modes, console logging is suppressed entirely (everything goes to the log file at the requested level)
- Fixed OutOfMemoryError and deadlock regressions in external annotation mode by bounding the worker backlog instead of letting whole-document annotation tasks accumulate unbounded in heap
- Annotation worker buffer sizing now respects `KORAPXMLTOOL_XMX`, so large configured heaps permit larger bounded backlogs without collapsing throughput
- Fixed Krill incremental output stalls where completed texts could be compressed but never written, causing the progress bar to stay near zero while memory kept growing
- Fixed Krill work-stealing queue selection to use month-aware `compareTextIds()` ordering instead of raw string order, preventing one foundry from starving the others
- Fixed a Krill writer-thread stall introduced by compression backpressure by keeping compression enqueueing on worker-completion paths instead of letting the writer thread run compressor work
- Fixed sparse and empty foundry handling regressions in Krill output by covering them with fixture-based tests (`ndy_sample.cmc.zip`, `ndy_sample.gender.zip`)

### Added

- Added `KRILL-STATS` log lines with heap usage, raw/compressed backlog sizes, in-flight compression count, ready-queue depth, and peak values for server-side tuning
- Added compression-pool diagnostics to `KRILL-STATS` (`active`, `queued`, `threads`) to distinguish writer stalls from compressor backlog

## [v3.2.1] - 2026-03-17

### Fixed

- Fixed OutOfMemoryError when annotating very long texts (novels) with ZIP output by streaming XML directly to output instead of materializing as String

## [v3.2.0] - 2026-03-15

### Fixed

- Fixed heap issues with krill conversion
- For krill output and morpho.xml files, bypass XMLCommentFilterReader
- Fixed progress bar showing the full path of the file instead of the file name ([#17](https://github.com/KorAP/korapxmltool/issues/17))

### Added

- Calculate heap demand for krill conversion
- Mark non-word tokens for index (necessary for distance operator compatibility with COSMAS II)

## [v3.1.3] - 2025-01-15

### Fixed

- Fixed `-L` (log directory) option being ignored when using internal taggers (`-T opennlp`, `-T marmot`, etc.)
- Renamed `textExternalLinks` metadata field to `textExternalLink` (singular) ([#26](https://github.com/KorAP/korapxmltool/issues/26))
- Use `rend` attribute as external link title, if available ([#27](https://github.com/KorAP/korapxmltool/issues/27))

## [v3.1.2] - 2025-12-18

### Fixed

- Fixed sparse annotations being assigned to wrong tokens by correctly handling the CoNLL-U ID column ([#23](https://github.com/KorAP/korapxmltool/issues/23))

## [v3.1.1] - 2025-12-17

### Fixed

- Compiler warnings about conditions that are always 'true' in metadata inheritance logic
- Missing metadata inherited from corpus and document headers
- Missing paragraph spans in Krill output
- Duplicate log files issue
- `-o` option failure with relative paths
- `-F` and `-T` option combination not working correctly
- `-o` option for `-T` with docker taggers
- Log to log files only

### Updated

- opennlp-tools from 2.5.6.1 to 2.5.7

## [v3.1.0] - 2025-12-04

### Fixed

- `-o <outputfile>` option, now has the highest priority for specifying the output file path
- missing newlines in file output for now, w2v, conllu target formats when using `-o` option

### Added

- integrated support for TreeTagger (`-T treetagger`) and spaCy (`-T spacy`) annotations
- automatically gzip output for now, w2v, conllu target formats if output file (`-o` option) ends with `.gz`

## [v3.0.0] - 2025-11-27

### Fixed
- **CLI**: Fixed `-o <output-file>` option consistency ([#12](https://github.com/KorAP/korapxmltool/issues/12)).
- **Badges**: Fixed release and build status badges in README.

### Changed

- **Annotation**: Multiple POS/lemma annotations are now sorted by probability in descending order.

### Updated

- **Dependencies**

## [v3.00-rc3] - 2025-11-22

### Added

- **Annotation**: Support multiple POS/lemma annotations ([#9](https://github.com/KorAP/korapxmltool/issues/9)).
- **CI/CD**: GitHub Actions for CI and Release workflows.
- **Documentation**: Added project status, lifecycle, release, and build status badges to README.

### Fixed
- **Annotation**: Fixed `-A -t zip` not picking up multiple POS interpretations ([#11](https://github.com/KorAP/korapxmltool/issues/11)).


## [v3.0-rc2] - 2025-11-21

### Fixed
- **Indexing/Krill**: Fixed emoji output for krill target ([#7](https://github.com/KorAP/korapxmltool/issues/7)).
- **Annotation**: Fixed external tool annotations (`-t zip -A`) not respecting foundry ([#8](https://github.com/KorAP/korapxmltool/issues/8)).

## [v3.0-rc1] - 2025-11-21

### Changed
- **CLI**: Standardized command line options:
    - `-j` / `--jobs` / `--threads` now sets the number of threads (replaced `-T`).
    - `-q` / `--quiet` suppresses progress output.
- **Documentation**: Updated README.md with new usage instructions and examples.
