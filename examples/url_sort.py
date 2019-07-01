import logging
import urllib.parse
import ext_sort as es


class Url(urllib.parse.ParseResult):

    def __lt__(self, other):
        return self.netloc < other.netloc


class UrlSerializer(es.Serializer):

    def write(self, item):
        self._writer.write(b"%s\n" % item.geturl())


class UrlDeserializer(es.Deserializer):

    def read(self):
        line = self._reader.readline()
        if not line:
            return None

        return Url._make(urllib.parse.urlparse(line.strip()))


logging.basicConfig(
    level=logging.DEBUG,
    format='[%(levelname)-8s] %(asctime)-15s (%(name)s): %(message)s',
)

with open('/home/user/urls.txt', 'rb') as unsorted_file, open('/home/user/urls.sorted.txt', 'wb') as sorted_file:
    es.sort(
        unsorted_file,
        sorted_file,
        chunk_size=1_000_000,
        Serializer=UrlSerializer,
        Deserializer=UrlDeserializer,
        workers_cnt=4,
    )
