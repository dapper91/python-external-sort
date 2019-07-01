import csv
import io
import logging

import ext_sort as es


class CSVSerializer(es.Serializer):

    def __init__(self, writer):
        super().__init__(csv.writer(io.TextIOWrapper(writer, write_through=True)))

    def write(self, item):
        return self._writer.writerow(item)


class CSVDeserializer(es.Deserializer):

    def __init__(self, reader):
        super().__init__(csv.reader(io.TextIOWrapper(reader)))

    def read(self):
        return next(self._reader)


logging.basicConfig(
    level=logging.DEBUG,
    format='[%(levelname)-8s] %(asctime)-15s (%(name)s): %(message)s',
)

with open('/home/user/data.csv', 'rb') as unsorted_file, open('/home/user/data.sorted.csv', 'wb') as sorted_file:
    # save the csv header
    sorted_file.write(unsorted_file.readline())

    es.sort(
        reader=unsorted_file,
        writer=sorted_file,
        chunk_size=1_000_000,
        Serializer=CSVSerializer,
        Deserializer=CSVDeserializer,
        workers_cnt=4,
    )
