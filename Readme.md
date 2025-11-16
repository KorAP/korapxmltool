# korapxmltool

Tool package to convert and annotate KorAP-XML ZIP files.

Up to 200 times faster and more accurate drop-in replacement for the korapxml2conllu part of [KorAP-XML-CoNLL-U](https://github.com/KorAP/KorAP-XML-CoNLL-U).

For some conversion tasks, however, you currently need the conllu2korapxml part of [KorAP-XML-CoNLL-U](https://github.com/KorAP/KorAP-XML-CoNLL-U).

## Download

You can download the latest jar build from the build artifacts [here](https://gitlab.ids-mannheim.de/KorAP/korapxml2conllu/-/jobs/artifacts/master/raw/app/build/libs/korapxmltool.jar?job=build).

## Build it yourself

```shell script
./gradlew build
```

## Conversion to [CoNLL-U format](https://universaldependencies.org/format.html)

```shell script
$ java  -jar ./app/build/libs/korapxmltool.jar app/src/test/resources/wdf19.zip | head -10

# foundry = base
# filename = WDF19/A0000/13072/base/tokens.xml
# text_id = WDF19_A0000.13072
# start_offsets = 0 0 14 17 25 30 35 42 44 52 60 73
# end_offsets = 74 12 16 24 29 34 41 43 51 59 72 74
1	Australasien	_	_	_	_	_	_	_	_
2	on	_	_	_	_	_	_	_	_
3	devrait	_	_	_	_	_	_	_	_
4	peut	_	_	_	_	_	_	_	_
5	être	_	_	_	_	_	_	_	_

```

## Conversion to language model training data input format from KorAP-XML

```shell script
$ java  -jar ./app/build/libs/korapxmltool.jar --word2vec t/data/wdf19.zip

Arts visuels Pourquoi toujours vouloir séparer BD et Manga ?
Ffx 18:20 fév 25 , 2003 ( CET ) soit on ne sépara pas , soit alors on distingue aussi , le comics , le manwa , le manga ..
la bd belge et touts les auteurs européens ..
on commence aussi a parlé de la bd africaine et donc ...
wikipedia ce prete parfaitement à ce genre de decryptage .
…
```

### Example producing language model training input with preceding metadata columns

```shell script
java  -jar ./app/build/libs/korapxmltool.jar  -m '<textSigle>([^<]+)' -m '<creatDate>([^<]+)' --word2vec t/data/wdf19.zip
```
```
WDF19/A0000.10894	2014.08.28	Arts visuels Pourquoi toujours vouloir séparer BD et Manga ?
WDF19/A0000.10894	2014.08.28	Ffx 18:20 fév 25 , 2003 ( CET ) soit on ne sépara pas , soit alors on distingue aussi , le comics , le manwa , le manga ..
WDF19/A0000.10894	2014.08.28	la bd belge et touts les auteurs européens ..
WDF19/A0000.10894	2014.08.28	on commence aussi a parlé de la bd africaine et donc ...
WDF19/A0000.10894	2014.08.28	wikipedia ce prete parfaitement à ce genre de decryptage .
```

### Conversion to a NOW corpus format variant (example)

One text per line with `<p>` as sentence delimiter.

```shell script
java -jar korapxmltool.jar -f now /vol/corpora/DeReKo/current/KorAP/zip/*24.zip | pv > dach24.txt
```

### Using lemmas instead of surface forms in word2vec / NOW output

If lemma annotations (morpho layer) are present alongside the base tokens, you can output lemmas instead of surface tokens with `--lemma`.

```shell script
# Word2Vec style output with lemmas where available
java -jar ./app/build/libs/korapxmltool.jar --lemma -f w2v app/src/test/resources/goe.tree_tagger.zip | head -3

# NOW corpus style output with lemmas
java -jar ./app/build/libs/korapxmltool.jar --lemma -f now app/src/test/resources/goe.tree_tagger.zip | head -1
```

If a lemma for a token is missing (`_`) the surface form is used as fallback.

### Lemma-only mode and I/O scheduling

- `--lemma-only`: For `-f w2v` and `-f now`, skip loading `data.xml` and output only lemmas from `morpho.xml`. This reduces memory and speeds up throughput.
- `--sequential`: Process entries inside each zip sequentially (zips can still run in parallel). Recommended for `w2v`/`now` to keep locality and lower memory.
- `--zip-parallelism N`: Limit how many zips are processed concurrently (defaults to `--threads`). Helps avoid disk thrash and native inflater pressure.
- `--exclude-zip-glob GLOB` (repeatable): Skip zip basenames that match the glob (e.g., `--exclude-zip-glob 'w?d24.tree_tagger.zip'`).

Example for large NOW export with progress and exclusions:

```
java -Xmx64G -XX:+UseG1GC -Djdk.util.zip.disableMemoryMapping=true -Djdk.util.zip.reuseInflater=true \
     -jar korapxmltool.jar -l info --threads 100 --zip-parallelism 8 \
     --lemma-only --sequential -f now \
     --exclude-zip-glob 'w?d24.tree_tagger.zip' \
     /vol/corpora/DeReKo/current/KorAP/zip/*24.tree_tagger.zip | pv > dach2024.lemma.txt
```

At INFO level the tool logs:
- The zip processing order with file sizes (largest-first in `--lemma-only`).
- For each zip: start message including its size and a completion line with cumulative progress, ETA and average MB/s.

### Conversion to Krill (KoralQuery) JSON format

Generate a tar archive containing gzipped Krill/KoralQuery JSON files across all provided foundries.

```shell script
java -jar ./app/build/libs/korapxmltool.jar -f krill -D out/krill \
  app/src/test/resources/wud24_sample.zip \
  app/src/test/resources/wud24_sample.spacy.zip \
  app/src/test/resources/wud24_sample.marmot-malt.zip
```

This writes `out/krill/wud24_sample.krill.tar` plus a log file. Add more annotated KorAP-XML zips (e.g., TreeTagger, CoreNLP) to merge their layers into the same Krill export; use `--non-word-tokens` if punctuation should stay in the token stream.

## Annotation

### Tagging with integrated MarMoT POS tagger directly to a new KorAP-XML ZIP file

You need to download the pre-trained MarMoT models from the [MarMoT models repository](http://cistern.cis.lmu.de/marmot/models/CURRENT/).

```shell script
java -jar ./app/build/libs/korapxmltool.jar -f zip -t marmot:models/de.marmot app/src/test/resources/goe.zip
```

### Tagging with integrated OpenNLP POS tagger directly to a new KorAP-XML ZIP file

You need to download the pre-trained OpenNLP models from the [OpenNLP model download page](https://opennlp.apache.org/models.html#part_of_speech_tagging) or older models from the [legacy OpenNLP models archive](http://opennlp.sourceforge.net/models-1.5/).
```shell script
java -jar ./app/build/libs/korapxmltool.jar -f zip -t opennlp:/usr/local/kl/korap/Ingestion/lib/models/opennlp/de-pos-maxent.bin /tmp/zca24.zip
```

### Tag and lemmatize with TreeTagger

This requires the [TreeTagger Docker Image with CoNLL-U Support](https://gitlab.ids-mannheim.de/KorAP/CoNLL-U-Treetagger).
Language models are downloaded automatically.

```shell script
java  -jar app/build/libs/korapxmltool.jar app/src/test/resources/wdf19.zip | docker run --rm -i korap/conllu2treetagger -l french | conllu2korapxml
```

### Tag and lemmatize with spaCy directly to a new KorAP-XML ZIP file

This requires the [spaCy Docker Image with CoNLL-U Support](https://gitlab.ids-mannheim.de/KorAP/sota-pos-lemmatizers) and is only available for German.

```shell script
java  -jar app/build/libs/korapxmltool.jar -T4 -A "docker run -e SPACY_USE_DEPENDENCIES=False --rm -i korap/conllu2spacy:latest 2> /dev/null" -f zip ./app/src/test/resources/goe.zip
```

### Tag, lemmatize and dependency parse with spaCy directly to a new KorAP-XML ZIP file

```shell script
java  -jar app/build/libs/korapxmltool.jar -T4 -A "docker run -e SPACY_USE_DEPENDENCIES=True --rm -i korap/conllu2spacy:latest 2> /dev/null" -f zip ./app/src/test/resources/goe.zip
```

### Tag, lemmatize and constituency parse with CoreNLP (3.X) directly to a new KorAP-XML ZIP file

Download the Stanford CoreNLP v3.X POS tagger and constituency parser models (e.g., `german-fast.tagger` and `germanSR.ser.gz`) into `libs/`.

```shell script
java -jar ./app/build/libs/korapxmltool.jar -f zip -D out \
  -t corenlp:libs/german-fast.tagger \
  -P corenlp:libs/germanSR.ser.gz \
  app/src/test/resources/wud24_sample.zip
```

The resulting `out/wud24_sample.corenlp.zip` contains `corenlp/morpho.xml` and `corenlp/constituency.xml` alongside the base tokens.

### Parse using the integrated Maltparser directly to a new KorAP-XML ZIP file

You need to download the pre-trained MaltParser models from the [MaltParser model repository](http://www.maltparser.org/mco/mco.html).
Note that parsers take POS tagged input.

```shell script
java -jar ./app/build/libs/korapxmltool.jar -f zip -T2 -P malt:libs/german.mco goe.tree_tagger.zip
```

### Tag with MarMoT and parse with Maltparser in one run directly to a new KorAP-XML ZIP file
```shell script
java -jar ./app/build/libs/korapxmltool.jar -f zip -t marmot:models/de.marmot -P malt:libs/german.mco goe.zip
```

## Development and License

**Author**:

* [Marc Kupietz](https://www.ids-mannheim.de/digspra/personal/kupietz.html)

Copyright (c) 2024-2025, [Leibniz Institute for the German Language](http://www.ids-mannheim.de/), Mannheim, Germany

This package is developed as part of the [KorAP](http://korap.ids-mannheim.de/)
Corpus Analysis Platform at the Leibniz Institute for German Language
([IDS](http://www.ids-mannheim.de/)).

It is published under the GNU General Public License, Version 3, 29 June 2007.

## Contributions

Contributions are very welcome!

Your contributions should ideally be committed via our [Gerrit server](https://korap.ids-mannheim.de/gerrit/)
to facilitate reviewing (
see [Gerrit Code Review - A Quick Introduction](https://korap.ids-mannheim.de/gerrit/Documentation/intro-quick.html)
if you are not familiar with Gerrit). However, we are also happy to accept comments and pull requests
via GitHub.
