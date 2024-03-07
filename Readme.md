# korapxml2conllu

Tool package to convert from KorAP XML format to [CoNLL-U format](https://universaldependencies.org/format.html), as
well as other simple formats, including token boundary information.

Up to 200 times faster and more accurate drop-in replacement for the korapxml2conllu part of [KorAP-XML-CoNLL-U](https://github.com/KorAP/KorAP-XML-CoNLL-U).


## Build

```shell script
./gradlew build
```

## Run

```shell script
$ java  -jar ./app/build/libs/korapxml2conllu.jar app/src/test/resources/wdf19.zip | head -10

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

### Example producing language model training input from KorAP-XML

```shell script
$ java  -jar ./app/build/libs/korapxml2conllu.jar --word2vec t/data/wdf19.zip

Arts visuels Pourquoi toujours vouloir séparer BD et Manga ?
Ffx 18:20 fév 25 , 2003 ( CET ) soit on ne sépara pas , soit alors on distingue aussi , le comics , le manwa , le manga ..
la bd belge et touts les auteurs européens ..
on commence aussi a parlé de la bd africaine et donc ...
wikipedia ce prete parfaitement à ce genre de decryptage .
…
```

### Example producing language model training input with preceding metadata columns

```shell script
java  -jar ./app/build/libs/korapxml2conllu.jar  -m '<textSigle>([^<]+)' -m '<creatDate>([^<]+)' --word2vec t/data/wdf19.zip
```

```
WDF19/A0000.10894	2014.08.28	Arts visuels Pourquoi toujours vouloir séparer BD et Manga ?
WDF19/A0000.10894	2014.08.28	Ffx 18:20 fév 25 , 2003 ( CET ) soit on ne sépara pas , soit alors on distingue aussi , le comics , le manwa , le manga ..
WDF19/A0000.10894	2014.08.28	la bd belge et touts les auteurs européens ..
WDF19/A0000.10894	2014.08.28	on commence aussi a parlé de la bd africaine et donc ...
WDF19/A0000.10894	2014.08.28	wikipedia ce prete parfaitement à ce genre de decryptage .
```

## Development and License

**Author**:

* [Marc Kupietz](https://www.ids-mannheim.de/digspra/personal/kupietz.html)

Copyright (c) 2024, [Leibniz Institute for the German Language](http://www.ids-mannheim.de/), Mannheim, Germany

This package is developed as part of the [KorAP](http://korap.ids-mannheim.de/)
Corpus Analysis Platform at the Leibniz Institute for German Language
([IDS](http://www.ids-mannheim.de/)).

It is published under the BSD 2-clause "Simplified" license.

## Contributions

Contributions are very welcome!

Your contributions should ideally be committed via our [Gerrit server](https://korap.ids-mannheim.de/gerrit/)
to facilitate reviewing (
see [Gerrit Code Review - A Quick Introduction](https://korap.ids-mannheim.de/gerrit/Documentation/intro-quick.html)
if you are not familiar with Gerrit). However, we are also happy to accept comments and pull requests
via GitHub.
