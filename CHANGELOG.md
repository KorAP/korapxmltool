# Changelog

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
