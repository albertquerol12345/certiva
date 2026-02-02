def test_dedupe_is_scoped_by_tenant(temp_certiva_env):
    utils = temp_certiva_env["utils"]
    current_date = utils.today_iso()
    utils.upsert_dedupe("doc_a", "tenant_a", "B11111111", "INV-001", current_date, 100)
    utils.upsert_dedupe("doc_b", "tenant_b", "B11111111", "INV-001", current_date, 100)

    duplicates_a = utils.find_duplicates("tenant_a", "B11111111", "INV-001", 100)
    assert len(duplicates_a) == 1
    assert duplicates_a[0]["doc_id"] == "doc_a"
    assert duplicates_a[0]["tenant"] == "tenant_a"

    duplicates_b = utils.find_duplicates("tenant_b", "B11111111", "INV-001", 100)
    assert len(duplicates_b) == 1
    assert duplicates_b[0]["doc_id"] == "doc_b"
    assert duplicates_b[0]["tenant"] == "tenant_b"

    cross = utils.find_duplicates("tenant_c", "B11111111", "INV-001", 100)
    assert cross == []
