# Changelog

## [Unreleased]

### Added

- Krill `externalLink` is now derived for sources where the full-text link is not explicitly encoded as a `ref[@type=page_url]`/`link` element, mirroring the predecessor tool (KorAP-XML-Krill): DGD/AGD/FOLK transcripts (title `_DF_<n>` suffix stripped, title "DGD"), Süddeutsche Zeitung (`U<yy>` sigles → szarchiv link from the biblNote `ID:`, title "Süddeutsche Zeitung"), and Genios newspapers (corpus sigle → `orikuerzel` via a built-in mapping + biblNote `ID:` → genios.de link, title "GENIOS"). Explicitly encoded links still take precedence. Link titles are carried via a new `externalLinkTitle` helper field.
- Sentence segmentation fallback: when a corpus has no `s` spans in `structure.xml` (common for TEI conversions of custom corpora), sentence boundaries now fall back to other segment-like TEI elements, in order of preference: `posting` (chat/CMC), `l` (verse line), `seg` (segment), `u` (utterance). This makes integrated taggers/parsers and KorAP sentence-based queries work on such corpora.
- New `dck_sample.zip` test resource: two-text excerpt from the CC BY licensed Dortmunder Chat-Korpus with custom `cmc` tokenization/annotations (in one text the `s` spans are removed to exercise the sentence fallback)
- Krill output now indexes extra inline `<w>`-level annotations from TEI-derived corpora as their own token layers, alongside POS (`p`) and lemma (`l`): `norm`, `orig`, `phon` and `trans` (read from the correspondingly named `<f>` features in `morpho.xml`). Each becomes a `foundry/<name>=tokens` layer with `foundry/<name>:<value>` terms (original casing preserved), grouped under the `morpho` foundry. The set is intentionally restricted to these four names to keep the Krill hot path fast. These layers are Krill-only and do not affect CoNLL-U output.
- Krill output now always emits an `availability` field, which is legally required for a KorAP instance to serve a text. It is inherited from the corpus or document header when not set on the text (a lower level overrides a higher one); if no value is found at any level it defaults to `unknown`, logged per text at `FINE` and summarised in a single closing warning with the affected text count.

### Fixed

- Krill `externalLink` is now picked up from older Wikipedia corpora (wpd17/wdd17) where the article URL is only present in the `reference[type=complete]` text rather than encoded as a `ref`/`link` element ([#47](https://github.com/KorAP/korapxmltool/issues/47)). The URL is extracted with title "Wikipedia".
- `tokenSource` in Krill JSON output no longer resolves to a stand-off annotation foundry ([#48](https://github.com/KorAP/korapxmltool/issues/48)): token source identification and token-map updates are now restricted to base archive processing, so stand-off annotation foundries can no longer overwrite the base tokenization spans or steal the `tokenSource`.
- Krill output now drops texts that contain no tokens instead of emitting empty, unindexable documents ([#46](https://github.com/KorAP/korapxmltool/issues/46)). Such texts (e.g. articles with empty `data.xml`/`base/tokens.xml`) are skipped with a per-text warning and a summary count; texts with at least one token are kept. New `m21_empty_sample.zip` regression fixture (one empty text, one single-token text).
- Corpora with custom tokenization and annotations inside the base ZIP (e.g. `cmc/morpho.xml` from TEI conversions, with no `base/tokens.xml`) are now handled correctly: the foundry is derived from the annotation folder name instead of the ZIP file name, so Krill output indexes the annotations (e.g. `cmc/p`, `cmc/l`) instead of silently dropping them, the token stream is no longer empty (`tokenSource` is set to e.g. `cmc#morpho`), and CoNLL-U output reports `# foundry = cmc` instead of `# foundry = base`
- External/Docker taggers (e.g. `treetagger`, `spacy`) and `--annotate-with` commands no longer adopt the input corpus's own foundry when annotating corpora that ship custom inline annotations (e.g. `gingko/morpho.xml`). Previously the `# foundry =` comment echoed back through the tool overrode the tagger's foundry, so `korapxmltool -T treetagger -t zip mtz13.zip` wrote `mtz13.gingko.zip` and overwrote the corpus's `gingko` annotations instead of producing `mtz13.tree_tagger.zip`. The annotation tool's foundry now takes precedence; the echoed comment is only used as a fallback for unrecognized tools (generic `annotated` foundry).

### Changed

- Krill metadata field names corrected by default: the misleadingly named `textClass` is now emitted as `dmozDomain` (DMOZ-based topic-domain classification) and `textDomain` as `idsColumn` (normalised newspaper column / Ressort). Pass `--legacy-field-names` to keep the historical names. Note: querying the corrected indices by the old names will require a need an accordingly configured Koral Mapper plugin to be active.

## [v3.4.0] - 2026-06-08

### Added

- Stand-off metadata support for Krill output: text-level metadata (e.g. Wikipedia topic-domain classifications, external links) is read from `<standOff>` XML files and folded into the Krill index. Such files are auto-detected among the inputs by content (no extra option needed) and joined to texts by `raw_text/@docid`. Classification layers become `type:keywords` fields named after the layer id; link layers become `type:attachement` fields. By default every category present in the file is indexed, leaving selection to the producing annotation or classification tool.

## [v3.3.3] - 2026-06-04

### Added

- Support standard TEI P5 archives by falling back to standard XML elements for author (with primary role priority), title, pubDate (including date with when attribute), and externalLink (link with target attribute) when typical customized I5 metadata elements are missing
- Extract corpusSigle, docSigle, and textSigle from ZIP entry path directories as a fallback for archives without explicit sigle tags in their header.xml files

### Fixed

- Krill metadata inheritance now ignores empty text-level `creatDate`/`pubDate` elements, inherits metadata consistently from corpus and document headers, and backfills `creationDate` and `pubDate` from each other so both dates are always present once either one is available
- Corpus and document headers now expose the same common Krill metadata fields for downstream text-level inheritance, including title/author-style fields and publication metadata
- External annotation with ZIP output no longer slows down progressively on very large corpora; the text-submission scheduler now avoids repeated text-order list scans and the hot document-processing path no longer forces periodic full GCs

### Changed

- Re-target Java compiler compatibility to Java 21 to support building on any host JDK >= 21 without requiring a strict Gradle JVM Toolchain version configuration


## [v3.3.2] - 2026-04-06

### Fixed

- Krill output now writes `pubDate` even when only partial `pubDate` information is available in `header.xml`, for example year-only values such as `1960` ([#39](https://github.com/KorAP/korapxmltool/issues/39))

## [v3.3.1] - 2026-04-05

### Fixed

- Plain text output modes now use the faster archive-order streaming ZIP path with `java.util.zip.ZipFile`: `NOW`, `Word2Vec`, and `CoNLL-U` when exactly one base ZIP is given. This removes the multi-minute startup delay on very large archives with huge entry counts, while multi-ZIP and foundry-paired `CoNLL-U` input stays on the ordered pipeline
- Streaming text-output diagnostics now log ZIP open time and first-output timing more explicitly, making it easier to distinguish ZIP indexing overhead from actual extraction work
- CoreNLP now uses the base tokenization and sentence boundaries instead of its own, preventing morpho/constituency drift on forms such as `Schüler:innen` ([#38](https://github.com/KorAP/korapxmltool/issues/38))
- Build warnings.

### Updated

- dependencies
- Gradle wrapper

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
