from functions.data_syncer import data_syncer


def test_data_syncer():
    syncer = data_syncer.DataSyncer(
        "https://datausa.io/api/data?drilldowns=Nation&measures=Population",
        "noventa-scratch-bucket",
    )
    
    assert syncer.s3_bucket == "noventa-scratch-bucket"
    assert syncer.url == "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
    assert syncer.prefix == "census_data"
