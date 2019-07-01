import abc
import contextlib
import heapq
import logging
import multiprocessing as mp
import tempfile

logger = logging.getLogger(__package__)


class Serializer(abc.ABC):
    """
    Abstract data serializer. Accepts an item of any type and transforms it to a byte string representation
    so that it can be written to a file on a hard drive. Thus in conjunction with `Deserializer` it helps
    to get an ability to sort items of any type since it provides a way to dump and load item without any change.
    Custom serializers should be inherited from it.
    """

    def __init__(self, writer):
        self._writer = writer

    @abc.abstractmethod
    def write(self, item):
        """
        Serializes an item to byte sting representation and writes it to the writer.

        :param item: item to be serialized
        """


class Deserializer(abc.ABC):
    """
    Abstract data deserializer. Reads a byte string from the reader and transforms it to an item of a custom type.
    Thus in conjunction with `Serializer` it helps to get an ability to sort items of any type since it provides
    a way to dump and load item without any change.
    Custom deserializers should be inherited from it.
    """

    def __init__(self, reader):
        self._reader = reader

    @abc.abstractmethod
    def read(self):
        """
        Reads an item from the reader as a byte string and deserializes it.

        :return: deserialized item
        """

    def __iter__(self):
        return self

    def __next__(self):
        item = self.read()
        if not item:
            raise StopIteration

        return item


class ByteLineDeserializer(Deserializer):
    """
    Byte line deserializer. Reads a byte line from the reader and returns it.
    """

    def read(self):
        return self._reader.readline().rstrip()


class ByteLineSerializer(Serializer):
    """
    Byte line serializer. Accept a byte string, append new line to it and writes the result to the writer.
    """

    def write(self, item):
        self._writer.write(item + b'\n')


def sort(
    reader, writer,
    chunk_size=None, chunk_mem=None, total_mem=None,
    Serializer=ByteLineSerializer,
    Deserializer=ByteLineDeserializer,
    workers_cnt=None,
    tmp_dir=None,
):
    """
    Sorts a file using external sort algorithm.

    :param reader: byte reader the input data is read from.
    :param writer: byte writer the output data is written to.
    :param chunk_size: maximum number of items in chunk that a worker will sort.
    :param chunk_mem: maximum main memory size that a worker will use. Not implemented yet.
    :param total_mem: maximum main memory size that sorting will use. Not implemented yet.
    :param Serializer: item serializer. If omitted `ByteLineSerializer` is used.
    :param Deserializer: item deserializer. If omitted `ByteLineDeserializer` is used.
    :param workers_cnt: number of workers sorting is performed in.
    :param tmp_dir: temporary directory path. If omitted platform default temp directory is used.
    """

    assert any((chunk_size, chunk_mem, total_mem)), "chunk_size, chunk_mem or total_mem must be provided"

    workers_cnt = workers_cnt or mp.cpu_count()

    logger.debug(f"sorting file using {workers_cnt} workers (chunk_size: {chunk_size})")

    with tempfile.TemporaryDirectory(dir=tmp_dir) as tmp_dir:
        logger.debug(f"using '{tmp_dir}' as temporary directory")

        with mp.Pool(workers_cnt) as pool:
            deserializer = Deserializer(reader)
            async_results = []

            chunk = []
            for item in deserializer:
                chunk.append(item)
                if len(chunk) == chunk_size:
                    chunk_filename = _flush_chunk_to_tmp_file(chunk, tmp_dir, Serializer)
                    async_results.append(pool.apply_async(_sort_file, kwds=dict(
                        filename=chunk_filename,
                        Serializer=Serializer,
                        Deserializer=Deserializer,
                    )))

            if len(chunk):
                chunk_filename = _flush_chunk_to_tmp_file(chunk, tmp_dir, Serializer)
                async_results.append(pool.apply_async(_sort_file, kwds=dict(
                    filename=chunk_filename,
                    Serializer=Serializer,
                    Deserializer=Deserializer,
                )))

            tmp_filenames = [res.get() for res in async_results]

        _merge_files(tmp_filenames, writer, Serializer, Deserializer)


def _sort_file(filename, Serializer, Deserializer):
    logger.debug(f"[{mp.current_process().name}] sorting file '{filename}'...")

    with open(filename, 'rb') as reader:
        data = [item for item in Deserializer(reader)]

    data = sorted(data)

    result_filename = f"{filename}.sorted"
    with open(result_filename, 'wb') as writer:
        serializer = Serializer(writer)

        for item in data:
            serializer.write(item)

    return result_filename


def _flush_chunk_to_tmp_file(chunk, tmp_dir, Serializer, filename_prefix='chunk-'):
    tmp_fd, tmp_filename = tempfile.mkstemp(prefix=filename_prefix, dir=tmp_dir)

    logger.debug(f"creating chunk file '{tmp_filename}'...")

    with open(tmp_fd, mode='wb') as writer:
        serializer = Serializer(writer)

        for item in chunk:
            serializer.write(item)

    chunk.clear()

    return tmp_filename


def _merge_files(filenames, writer, Serializer, Deserializer):
    logger.debug(f"merging result...")

    with contextlib.ExitStack() as stack:
        tmp_files = [Deserializer(stack.enter_context(open(filename, mode='rb'))) for filename in filenames]

        serializer = Serializer(writer)
        for item in heapq.merge(*tmp_files):
            serializer.write(item)
