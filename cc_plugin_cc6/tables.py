def retrieve(url, fname, path):
    import pooch

    # URL to one of Pooch's test files
    filename = pooch.retrieve(
        url=url,
        fname=fname,
        known_hash=None,
        path=path,
    )
    return filename
