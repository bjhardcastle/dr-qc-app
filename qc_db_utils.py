import datetime
import functools
import logging
import random
import time
from enum import IntEnum
from typing import Generator

import polars as pl
import upath

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

QC_DIR = upath.UPath("//allen/programs/mindscope/workgroups/dynamicrouting/qc")

DB_PATH = "//allen/programs/mindscope/workgroups/dynamicrouting/ben/qc_app.parquet"

CACHED_DF_PATH = "s3://aind-scratch-data/dynamic-routing/cache/nwb_components/{}/consolidated/{}.parquet"
CACHE_VERSION = "v0.0.265"


@functools.cache
def get_session_df() -> pl.DataFrame:
    return (
        pl.scan_parquet(CACHED_DF_PATH.format(CACHE_VERSION, "session"))
        .join(
            other=pl.scan_parquet(CACHED_DF_PATH.format(CACHE_VERSION, "subject")),
            on="session_id",
            how="inner",
        )
        .select("session_id", "keywords", "genotype")
        .collect()
    )


def generate_qc_df() -> pl.DataFrame:
    paths = [
        p.as_posix()
        for p in QC_DIR.rglob("*")
        if not p.is_dir() and not p.name == "Thumbs.db"
    ]
    path_list = pl.col("qc_path").str.split(".").list.get(0).str.split("/").list
    df = (
        pl.DataFrame({"qc_path": paths})
        .with_columns(
            qc_group=path_list.get(-3).cast(pl.Categorical),
            plot_name=path_list.get(-2),
            session_id=path_list.get(-1).str.split("_").list.slice(0, 2).list.join("_"),
            extension=pl.concat_str(
                pl.lit("."), (pl.col("qc_path").str.split(".").list.get(-1))
            ).cast(pl.Categorical),
        )
        .with_columns(
            plot_index=path_list.get(-1)
            .str.strip_prefix(pl.col("session_id"))
            .str.strip_prefix("_"),
        )
        .with_columns(
            (
                pl.when(pl.col("plot_index") == "")
                .then(pl.lit(None))
                .otherwise(pl.col("plot_index"))
                .alias("plot_index")
                .cast(pl.Int8)
            ),
            qc_rating=pl.lit(None).cast(pl.Int8),
            checked_timestamp=pl.lit(None).cast(pl.Datetime),
        )
    )
    return df


class QCRating(IntEnum):
    FAIL = 0
    PASS = 1
    UNSURE = 5


class Lock:
    def __init__(self, path=DB_PATH + ".lock"):
        self.path = upath.UPath(path)

    def acquire(self):
        while self.path.exists():
            time.sleep(0.01)
        self.path.touch()

    def release(self):
        upath.UPath(self.path).unlink()

    def __enter__(self):
        self.acquire()
        logger.info(f"Acquired lock at {self.path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        logger.info(f"Released lock at {self.path}")


def create_db(
    save_path: str = DB_PATH,
    overwrite: bool = False,
) -> pl.DataFrame:
    if upath.UPath(save_path).exists() and not overwrite:
        raise FileExistsError(
            f"{save_path} already exists: set overwrite=True to overwrite"
        )
    df = generate_qc_df()
    df.write_parquet(save_path)
    return df


def update_db(db_path: str = DB_PATH) -> None:
    upath.UPath(
        db_path + f".backup.{datetime.datetime.now():%Y%m%d_%H%M%S}"
    ).write_bytes(upath.UPath(db_path).read_bytes())
    temp_path = db_path + ".temp"
    # get a brand new df with up-to-date paths
    new_df = create_db(save_path=temp_path, overwrite=True).join(
        other=(
            (original_df := pl.read_parquet(db_path)).select(
                "qc_rating", "checked_timestamp"
            )
        ),
        on=["session_id", "qc_group", "plot_name", "plot_index"],
        how="left",
    )
    new_df.write_parquet(db_path)
    logger.info(f"Updated {db_path}: {len(original_df)} -> {len(new_df)} rows")
    upath.UPath(temp_path).unlink()


def reset_db(src_path: str, dest_path: str) -> None:
    pl.read_parquet(src_path).with_columns(
        qc_rating=pl.lit(None), checked_timestamp=pl.lit(None)
    ).write_parquet(dest_path)


def get_db(
    path_filter: str | None = None,
    already_checked: bool | None = None,
    qc_group_filter: str | None = None,
    plot_name_filter: str | None = None,
    plot_index_filter: int | None = None,
    extension_filter: str | None = '.png',
    session_id_filter: str | None = None,
    qc_rating_filter: int | None = None,
    db_path=DB_PATH,
) -> pl.DataFrame:
    filter_exprs = []
    if path_filter:
        filter_exprs.append(pl.col("qc_path").str.contains(path_filter))
    if already_checked is True and qc_rating_filter is None:
        filter_exprs.append(pl.col("qc_rating").is_not_null())
    elif already_checked is False and qc_rating_filter is None:
        filter_exprs.append(pl.col("qc_rating").is_null())
    if session_id_filter:
        filter_exprs.append(pl.col("session_id").str.starts_with(session_id_filter))
    if plot_name_filter:
        filter_exprs.append(pl.col("plot_name") == plot_name_filter)
    elif qc_group_filter:
        filter_exprs.append(pl.col("qc_group") == qc_group_filter)
    if plot_index_filter is not None:
        filter_exprs.append(pl.col("plot_index") == plot_index_filter)
    if extension_filter:
        filter_exprs.append(pl.col("extension") == extension_filter)
    if qc_rating_filter is not None:
        filter_exprs.append(pl.col("qc_rating") == int(qc_rating_filter))
    if filter_exprs:
        logger.info(
            f"Filtering qc df with {' & '.join([str(f) for f in filter_exprs])}"
        )
    if not filter_exprs:
        filter_exprs = [pl.lit(True)]
    return pl.read_parquet(db_path).filter(*filter_exprs)


def qc_item_generator(
    path_filter: str | None = None,
    already_checked: bool | None = None,
    qc_group_filter: str | None = None,
    plot_name_filter: str | None = None,
    plot_index_filter: int | None = None,
    extension_filter: str | None = '.png',
    session_id_filter: str | None = None,
    qc_rating_filter: int | None = None,
    db_path=DB_PATH,
) -> Generator[str, None, None]:
    while True:
        df = get_db(
            path_filter=path_filter,
            already_checked=already_checked,
            qc_group_filter=qc_group_filter,
            plot_name_filter=plot_name_filter,
            plot_index_filter=plot_index_filter,
            extension_filter=extension_filter,
            session_id_filter=session_id_filter,
            qc_rating_filter=qc_rating_filter,
            db_path=db_path,
        ).sort(pl.col("session_id").sort(descending=True), "qc_group", "plot_name", "plot_index")
        if len(df) == 0:
            raise StopIteration(f"No QC items to check matching filter: {path_filter=}, {already_checked=}, {qc_group_filter=}, {plot_name_filter=}, {plot_index_filter=}, {extension_filter=}, {session_id_filter=}, {qc_rating_filter=}")
        for row in df.iter_rows(named=True):
            yield str(row["qc_path"])  # cast to str for mypy


def set_qc_rating_for_path(path: str, qc_rating: int, db_path=DB_PATH) -> None:
    timestamp = int(time.time())
    original_df = get_db()
    path_filter = pl.col("qc_path") == path
    logger.info(f"Updating row for {path} with qc_rating={qc_rating}")
    df = original_df.with_columns(
        qc_rating=pl.when(path_filter)
        .then(pl.lit(qc_rating))
        .otherwise(pl.col("qc_rating")),
        checked_timestamp=pl.when(path_filter)
        .then(pl.lit(timestamp))
        .otherwise(pl.col("checked_timestamp")),
    )
    assert len(df) == len(
        original_df
    ), f"Row count changed: {len(original_df)} -> {len(df)}"
    with Lock():
        df.write_parquet(db_path)
    logger.info(f"Overwrote {db_path}")


def test_db(with_paths=False):
    db_path = "test.parquet"
    create_db(overwrite=True, save_path=db_path, with_paths=with_paths)
    i = next(qc_item_generator(db_path=db_path))
    set_qc_rating_for_path(i, 2, db_path=db_path)
    df = get_db(qc_group_filter=i, already_checked=True, db_path=db_path)
    assert len(df) == 1, f"Expected 1 row, got {len(df)}"
    upath.UPath(db_path).unlink()


if __name__ == "__main__":
    # generate_qc_df()
    create_db(overwrite=True)
    print(get_db().schema)
    # update_db()
    # print(get_df(ks4_filter=True).drop_nulls('path'))
