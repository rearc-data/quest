from functions.file_syncer import file_syncer


def test_file_syncer():
    syncer = file_syncer.FileSyncer(
        "https://download.bls.gov",
        "noventa-scratch-bucket",
    )

    assert syncer.host_url == "https://download.bls.gov"
    assert syncer.s3_bucket == "noventa-scratch-bucket"
    assert syncer.prefix == "productivity_cost"

    