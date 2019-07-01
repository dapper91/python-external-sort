import io
import pytest
import ext_sort as es


@pytest.mark.parametrize('chunk_size', (10, 30, 50, 100, 150))
@pytest.mark.parametrize('workers_cnt', (1, 2, 4, None))
def test_sort(freezed_random, chunk_size, workers_cnt):
    sorted_data = list(map(lambda n: f"{n}\n", range(100, 200)))

    unsorted_data = sorted_data.copy()
    freezed_random.shuffle(unsorted_data)

    reader = io.BytesIO(''.join(unsorted_data).encode())
    writer = io.BytesIO()

    es.sort(reader, writer, chunk_size=chunk_size, workers_cnt=workers_cnt)

    expected_result = ''.join(sorted_data).encode()

    writer.seek(0)
    actual_result = writer.read()

    assert actual_result == expected_result
