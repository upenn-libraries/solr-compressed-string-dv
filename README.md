## Motivation
Publish/export direct out of Solr has the benefit of supporting export in
conjunction with result sets defined by arbitrary queries. This is useful for
exporting subsets of data, including, e.g., incremental exports over arbitrary
"update date" ranges.

The access pattern inherent in export (and the associated higher-level
"streaming expressions" abstraction) requires that exported fields be configured
with [`docValues=true`](https://lucene.apache.org/solr/guide/8_5/exporting-result-sets.html#field-requirements).
For the common case where it is desirable to publish/export full source metadata,
this presents a problem when source metadata exceeds 32766 bytes (the maximum
supported size for Solr's `StrField` field type). A common workaround for this
situation in the non-export case is to use `TextField`, which is unlimited in size;
but `TextField` does not support docValues, and is thus not appropriate for the
export use case.

This custom fieldType extends `StrField` with support for deflate compression with 
optional (highly recommended) "dictionary" file. The initial POC, used with MARCXML
data, supported average compression to around 20% of the raw size, easily buying
enough headroom to use in practice (as an extension of `StrField`) with docValues
enabled.

## Use

Configure the field type as usual in `schema.xml`:

```xml
<fieldType name="string_compressed" class="solr.CompressedStrField"
           indexed="false"
           stored="false"
           docValues="true"
           dictionaryFile="default_marcxml_deflate_dictionary.txt"
           compressOnlyWhenNecessary="false"
           compressionLevel="9"/>
```
All compression-specific args are optional:
* `dictionaryFile` is a path to a resource file containing common character sequences,
used to prime the deflate dictionary. If your destination field has relatively predictable
content, this file can greatly improve compression, especially in the case of short field
values. Some useful discussion can be found [here](https://blog.cloudflare.com/improving-compression-with-preset-deflate-dictionary/).
The tool linked from that discussion was used over a naive subset of MARCXML records to generate
the dictionary file included in this distribution; the resulting file is almost
certainly sub-optimal. YMMV. If not specified, the value of this arg defaults to `null` (no
dictionary file is used). The file used at compression (indexing) time _must_ be identical
to the file used at decompression (query) time.
* `compressOnlyWhenNecessary`, if set to `true`, will cause compression to be applied _iff_
the raw (uncompressed) input would exceed the maximum threshold for `StrField` 32766 bytes.
* `compressionLevel` defaults to `9` ("best" compression). Lower values (down to `1`) may
increase encoding speed, at the expense of compression.


Once the fieldType is defined in your schema, it may be used to define fields in the same way
as any other fieldType.

## Background, caveats
`CompressedStrField` is intended to support "export" and "useDocValuesAsStored"
uses of docValues. Other uses of docValues (e.g., sorting, faceting) would in any case
be meaningless for the full-text metadata fields that are the intended use case.

It is still of course possible to exceed the headroom that is bought by compression.
While the nature of compression makes it impossible to determine a hard threshold/cutoff
for what size input will be "too big", in practice (depending on your data) the extra
headroom is likely to be sufficient.

I believe the reason for the size threshold on `StrField` is that it uses an
ordinal-based serialization data structure optimized for sorting, shared prefixes, and
repeated values. I believe the benefits conferred by the `StrField` data structure
are irrelevant for the "export" and "useDocValuesAsStored" cases, and it would be
preferable (and readily achievable, at least for the single-valued case) to introduce
a "string"-ish fieldType with no size limit, based over simple `BinaryDocValues` (as
opposed to `SortedDocValues` or `SortedSetDocValues`). I think in the longer term, this
`BinaryDocValues` approach is the complete, correct approach.
