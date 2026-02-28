import datetime
import io
import json
import logging
import re
from typing import Generator, NotRequired, TypedDict

import altair as alt
import npc_io
import panel as pn
import panel.custom
import param
import PIL.Image
import polars as pl
import upath

import qc_db_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  #! doesn't take effect

# pn.config.theme = 'dark'
pn.config.notifications = True


SIDEBAR_WIDTH = 280
BUTTON_WIDTH = int(SIDEBAR_WIDTH * 0.8)

NAIVE_DB_PATH = qc_db_utils.DB_PATH.replace("qc_app", "qc_naive_app")
if not upath.UPath(NAIVE_DB_PATH).exists():
    qc_db_utils.create_db(save_path=NAIVE_DB_PATH, naive_only=True)
# else:
#     qc_db_utils.update_db(db_path=NAIVE_DB_PATH, naive_only=True)

db_df: pl.DataFrame = qc_db_utils.get_db(
    extension_filter=None, naive_only=True, db_path=NAIVE_DB_PATH
)


def get_metrics(qc_path: str):
    return (
        qc_db_utils.get_db(db_path=NAIVE_DB_PATH, extension_filter=None)
        .filter(pl.col("qc_path") == qc_path)
        .select("session_id", "qc_group", "plot_name", "plot_index")
        .join(
            other=qc_db_utils.get_session_df(),
            on="session_id",
            how="left",
        )
        .to_dicts()[0]
    )


def display_metrics(
    path=str,
) -> pn.pane.Markdown:
    metrics = get_metrics(path)
    stats = f"""

`{path}`
"""
    for k, v in metrics.items():
        if isinstance(v, float):
            v = f"{v:.2f}"
        stats += f"\n{k.replace('_', ' ')}:\n`{v if v else '-'}`\n"
    return pn.pane.Markdown(stats, width=SIDEBAR_WIDTH)


def display_rating(
    path=str,
) -> pn.pane.Markdown:
    df = qc_db_utils.get_db(db_path=NAIVE_DB_PATH, extension_filter=None).filter(
        pl.col("qc_path") == path
    )
    assert len(df) == 1, f"Expected 1 row, got {len(df)}"
    row = df.to_dicts()[0]
    rating: int | None = row["qc_rating"]
    if rating is None:
        return pn.pane.Markdown("not yet rated")
    else:
        return pn.pane.Markdown(
            f"**{qc_db_utils.QCRating(rating).name.title()}** ({datetime.datetime.fromtimestamp(row['checked_timestamp']).strftime('%Y-%m-%d %H:%M:%S')})"
        )


def concatenate_pngs_vertically(png1_bytes: bytes, png2_bytes: bytes) -> bytes:
    """Concatenates two PNG images vertically and returns the combined image as bytes."""
    img1 = PIL.Image.open(io.BytesIO(png1_bytes))
    img2 = PIL.Image.open(io.BytesIO(png2_bytes))

    # Ensure both images are in RGB format
    img1 = img1.convert("RGB")
    img2 = img2.convert("RGB")

    width1, height1 = img1.size
    width2, height2 = img2.size

    # Scale img2 to the width of img1
    if width2 != width1:
        img2 = img2.resize(
            (width1, int(height2 * (width1 / width2))), PIL.Image.Resampling.LANCZOS
        )
        width2, height2 = img2.size  # Update dimensions after resizing

    # Create a new image with the combined height and maximum width
    new_img = PIL.Image.new("RGB", (width1, height1 + height2))

    # Paste the images into the new image
    new_img.paste(img1, (0, 0))
    new_img.paste(img2, (0, height1))

    # Save the combined image to bytes
    output_buffer = io.BytesIO()
    new_img.save(output_buffer, format="PNG")
    return output_buffer.getvalue()


def display_item(qc_path: str, second_image: str | None = None):
    path = upath.UPath(qc_path)
    logger.info(f"Displaying {path}")
    if path.suffix == ".png":
        if second_image:
            image_bytes = concatenate_pngs_vertically(
                path.read_bytes(), upath.UPath(second_image).read_bytes()
            )
        else:
            image_bytes = path.read_bytes()
        return pn.pane.PNG(
            image_bytes,
            sizing_mode="stretch_both",
        )
    if path.suffix == ".json":
        return pn.pane.JSON(
            json.loads(path.read_bytes()),
            sizing_mode="stretch_height",
        )
    if path.suffix in (".txt", ".error"):
        return pn.pane.HTML(
            f"<pre>{path.read_text()}</pre>",
            sizing_mode="stretch_height",
        )
    if path.suffix == ".link":
        link = upath.UPath(path.read_text())
        if link.as_posix().startswith("http"):
            return pn.pane.HTML(
                f'<a href="{link.read_text()}">Link</a>',
                sizing_mode="stretch_height",
            )
        if link.as_posix().startswith("s3://"):
            return pn.pane.Video(
                npc_io.get_presigned_url(link), loop=False, autoplay=True, width=1600
            )


# initialize for startup, then update as a global variable
qc_path_generator = qc_db_utils.qc_item_generator(
    already_checked=False,
    db_path=NAIVE_DB_PATH,
)
current_qc_path = next(qc_path_generator)
previous_qc_path: str | None = None
metrics_pane = display_metrics(current_qc_path)
qc_rating_pane = display_rating(current_qc_path)
qc_item_pane = pn.Column(display_item(current_qc_path), sizing_mode="stretch_both")


def next_path(override_next_path: str | None = None) -> None:
    global current_qc_path, previous_qc_path
    if override_next_path:
        current_qc_path = override_next_path
        undo_button.disabled = True
    else:
        previous_qc_path = current_qc_path
        undo_button.disabled = False
        try:
            current_qc_path = next(qc_path_generator)
        except StopIteration:
            pn.state.notifications.warning("No matching paths")
    global qc_item_pane

    second_image = None
    if "drift_maps" in current_qc_path:
        running_df = db_df.filter(
            pl.col("session_id") == get_metrics(current_qc_path)["session_id"],
            pl.col("plot_name") == "running",
            pl.col("extension") == ".png",
        )
        if len(running_df) > 0:
            second_image = str(running_df["qc_path"].first())
    qc_item_pane[:] = [display_item(current_qc_path, second_image=second_image)]
    metrics_pane.object = display_metrics(current_qc_path).object
    qc_rating_pane.object = display_rating(current_qc_path).object
    update_button_states()


def update_and_next(path: str, qc_rating: int) -> None:
    qc_db_utils.set_qc_rating_for_path(
        path=path, qc_rating=qc_rating, db_path=NAIVE_DB_PATH
    )
    next_path()


# make three buttons for rating qc item
no_button_text = (
    f"{qc_db_utils.QCRating.FAIL.name.title()} [{qc_db_utils.QCRating.FAIL.value}]"
)
no_button = pn.widgets.Button(name=no_button_text, width=BUTTON_WIDTH)
no_button.on_click(
    lambda event: update_and_next(
        path=current_qc_path, qc_rating=qc_db_utils.QCRating.FAIL
    )
)
yes_button_text = (
    f"{qc_db_utils.QCRating.PASS.name.title()} [{qc_db_utils.QCRating.PASS.value}]"
)
yes_button = pn.widgets.Button(name=yes_button_text, width=BUTTON_WIDTH)
yes_button.on_click(
    lambda event: update_and_next(
        path=current_qc_path, qc_rating=qc_db_utils.QCRating.PASS
    )
)
unsure_button_text = (
    f"{qc_db_utils.QCRating.UNSURE.name.title()} [{qc_db_utils.QCRating.UNSURE.value}]"
)
unsure_button = pn.widgets.Button(name=unsure_button_text, width=BUTTON_WIDTH)
unsure_button.on_click(
    lambda event: update_and_next(
        path=current_qc_path, qc_rating=qc_db_utils.QCRating.UNSURE
    )
)
skip_button = pn.widgets.Button(name="Skip [s]", width=BUTTON_WIDTH)
skip_button.on_click(lambda event: next_path())
undo_button = pn.widgets.Button(name="Previous [p]", width=BUTTON_WIDTH)
undo_button.disabled = True
undo_button.on_click(lambda event: next_path(override_next_path=previous_qc_path))


def update_button_states():
    is_error = upath.UPath(current_qc_path).suffix == ".error"
    no_button.disabled = is_error
    yes_button.disabled = is_error
    unsure_button.disabled = is_error


# - ---------------------------------------------------------------- #
# from https://github.com/holoviz/panel/issues/3193#issuecomment-2357189979


# Note: this uses TypedDict instead of Pydantic or dataclass because Bokeh/Panel doesn't seem to
# like serializing custom classes to the frontend (and I can't figure out how to customize that).
class KeyboardShortcut(TypedDict):
    name: str
    key: str
    altKey: NotRequired[bool]
    ctrlKey: NotRequired[bool]
    metaKey: NotRequired[bool]
    shiftKey: NotRequired[bool]


class KeyboardShortcuts(panel.custom.ReactComponent):
    """
    Class to install global keyboard shortcuts into a Panel app.

    Pass in shortcuts as a list of KeyboardShortcut dictionaries, and then handle shortcut events in Python
    by calling `on_msg` on this component. The `name` field of the matching KeyboardShortcut will be sent as the `data`
    field in the `DataEvent`.

    Example:
    >>> shortcuts = [
        KeyboardShortcut(name="save", key="s", ctrlKey=True),
        KeyboardShortcut(name="print", key="p", ctrlKey=True),
    ]
    >>> shortcuts_component = KeyboardShortcuts(shortcuts=shortcuts)
    >>> def handle_shortcut(event: DataEvent):
            if event.data == "save":
                print("Save shortcut pressed!")
            elif event.data == "print":
                print("Print shortcut pressed!")
    >>> shortcuts_component.on_msg(handle_shortcut)
    """

    shortcuts = param.List(class_=dict)

    _esm = """
    // Hash a shortcut into a string for use in a dictionary key (booleans / null / undefined are coerced into 1 or 0)
    function hashShortcut({ key, altKey, ctrlKey, metaKey, shiftKey }) {
      return `${key}.${+!!altKey}.${+!!ctrlKey}.${+!!metaKey}.${+!!shiftKey}`;
    }

    export function render({ model }) {
      const [shortcuts] = model.useState("shortcuts");

      const keyedShortcuts = {};
      for (const shortcut of shortcuts) {
        keyedShortcuts[hashShortcut(shortcut)] = shortcut.name;
      }

      function onKeyDown(e) {
        // Ignore key presses in input, textarea, or content-editable elements
        const target = e.target;
        const isInput = target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.isContentEditable;
        if (isInput) {
          return;
        }
        const name = keyedShortcuts[hashShortcut(e)];
        if (name) {
          e.preventDefault();
          e.stopPropagation();
          model.send_msg(name);
          return;
        }
      }

      React.useEffect(() => {
        window.addEventListener('keydown', onKeyDown);
        return () => {
          window.removeEventListener('keydown', onKeyDown);
        };
      });

      return <></>;
    }
    """


shortcuts = [
    KeyboardShortcut(name="skip", key="s", ctrlKey=False),
    KeyboardShortcut(name="previous", key="p", ctrlKey=False),
] + [
    KeyboardShortcut(name=k, key=str(v), ctrlKey=False)
    for k, v in qc_db_utils.QCRating.__members__.items()
]
shortcuts_component = KeyboardShortcuts(shortcuts=shortcuts)


def handle_shortcut(event):
    if event.data == "skip":
        next_path()
    elif event.data == "previous":
        next_path(override_next_path=previous_qc_path)
    else:
        if upath.UPath(current_qc_path).suffix == ".error":
            return  # don't rate error items
        update_and_next(
            path=current_qc_path, qc_rating=qc_db_utils.QCRating[event.data]
        )


shortcuts_component.on_msg(handle_shortcut)


chart = (
    alt.Chart(
        data=(
            db_df.filter(pl.col("extension").is_in([".png", ".error"]))
            .with_columns(
                pl.when(pl.col("extension").eq(".error"))
                .then(pl.lit(9))
                .otherwise(pl.col("qc_rating"))
                .alias("qc_rating")
            )
            .group_by("plot_name", "qc_group")
            .agg(
                pl.col("qc_rating").value_counts(),
            )
            .explode("qc_rating")
            .unnest("qc_rating")
            .sort("qc_group")
            .with_columns(
                pl.col("qc_rating")
                .replace_strict(
                    {0: "fail", 1: "pass", 5: "unsure", 9: "error"}, default="unrated"
                )
                .alias("rating"),
            )
            .with_columns(
                pl.concat_str(["qc_group", "plot_name"], separator="/").alias("qc_item")
            )
        ).to_pandas()
    )
    .mark_bar()
    .encode(
        x="qc_item:N",
        xOffset=alt.XOffset("qc_rating:N"),
        y=alt.Y("count").scale(type="symlog"),
        color=alt.Color("rating:N").scale(
            domain=["fail", "pass", "unsure", "unrated", "error"],
            range=["red", "green", "orange", "lightgrey", "pink"],
        ),
        tooltip=["plot_name", "qc_group", "rating", "count"],
    )
    .properties(
        width=1200,
    )
)
# - ---------------------------------------------------------------- #


def update_path_generator(event) -> None:
    global qc_path_generator

    def handle_no_filter(v):
        return v if v != "no filter" else None

    probe_filter = re.search(r"probe[A-F]", plot_name_filter_dropdown.value)
    print(probe_filter)
    showing_errors = show_errors_toggle.value
    qc_rating_filter_radio.disabled = showing_errors
    path_generator_params = dict(
        qc_rating_filter=qc_rating_filter_radio.value,
        session_id_filter=handle_no_filter(session_id_filter_dropdown.value),
        plot_name_filter=(
            handle_no_filter(plot_name_filter_dropdown.value).split("/")[-1]
            if handle_no_filter(plot_name_filter_dropdown.value) is not None
            else None
        ),
        probe_filter=probe_filter[0] if probe_filter else None,
        extension_filter=".error" if showing_errors else ".png",
    )
    print(path_generator_params)
    if showing_errors:
        # errors can't be rated, so ignore rating filters
        path_generator_params["already_checked"] = None
        path_generator_params["qc_rating_filter"] = None
    elif qc_rating_filter_radio.value == "unrated":
        path_generator_params["already_checked"] = False
        path_generator_params["qc_rating_filter"] = None
    elif qc_rating_filter_radio.value == "all":
        path_generator_params["already_checked"] = None
        path_generator_params["qc_rating_filter"] = None
    else:
        path_generator_params["qc_rating_filter"] = qc_db_utils.QCRating[
            qc_rating_filter_radio.value.upper()
        ].value
    logger.info(f"Updating path generator with {path_generator_params}")
    qc_path_generator = qc_db_utils.qc_item_generator(
        **path_generator_params, db_path=NAIVE_DB_PATH
    )
    next_path()


plot_name_filter_dropdown = pn.widgets.Select(
    name="Plot name",
    value="no filter",
    options=(
        db_df.filter(pl.col("extension").is_in([".png", ".error"]))
        .select(pl.concat_str(["qc_group", "plot_name"], separator="/").alias("name"))
        .get_column("name")
        .unique()
        .sort(descending=True)
        .to_list()
        + ["no filter"]
    ),
)
plot_name_filter_dropdown.param.watch(update_path_generator, "value")

session_id_filter_dropdown = pn.widgets.Select(
    name="Session ID",
    value="no filter",
    options=db_df.filter(pl.col("extension").is_in([".png", ".error"]))["session_id"]
    .unique()
    .sort(descending=False)
    .to_list()
    + ["no filter"],
    width=BUTTON_WIDTH,
)
session_id_filter_dropdown.param.watch(update_path_generator, "value")
qc_rating_filter_radio = pn.widgets.RadioBoxGroup(
    name="Show rated items",
    options=["all", "unrated"] + [k.lower() for k in qc_db_utils.QCRating.__members__],
    inline=False,
    value="unrated",
)
qc_rating_filter_radio.param.watch(update_path_generator, "value")

show_errors_toggle = pn.widgets.Toggle(
    name="Show errors",
    value=False,
    width=BUTTON_WIDTH,
)
show_errors_toggle.param.watch(update_path_generator, "value")


# toggle to switch between filter on current session ID and all session IDs/previous QC plot
def toggle_session_id_filter_generator() -> Generator:
    # save state of current UI elements so we can restore them later
    original_plot_name = plot_name_filter_dropdown.value
    original_qc_rating = qc_rating_filter_radio.value
    yield
    # restore state of UI elements
    yield original_plot_name, original_qc_rating


toggle_generator = None


def apply_session_id_filter(event) -> None:
    global toggle_generator
    global qc_path_generator
    if toggle_generator is None:
        toggle_generator = toggle_session_id_filter_generator()
        toggle_generator.send(None)
        session_id = get_metrics(current_qc_path)["session_id"]
        qc_rating_filter_radio.value = "all"
        session_id_filter_dropdown.value = session_id
        session_id_filter_dropdown.disabled = True
        session_id_filter_button.button_type = "primary"
        session_id_filter_button.name = "Restore previous view"
        update_path_generator(event)
    else:
        # restore state of UI elements
        original_plot_name, original_qc_rating = next(toggle_generator)
        session_id_filter_dropdown.value = "no filter"
        plot_name_filter_dropdown.value = original_plot_name
        qc_rating_filter_radio.value = original_qc_rating
        session_id_filter_dropdown.disabled = False
        session_id_filter_button.button_type = "default"
        session_id_filter_button.name = "Filter for current session"
        toggle_generator = None


session_id_filter_button = pn.widgets.Toggle(
    name="Filter for current session",
    width=BUTTON_WIDTH,
)
session_id_filter_button.param.watch(apply_session_id_filter, "value")


def update_db_callback(event) -> None:
    """Update the database with visual feedback."""
    update_db_button.disabled = True
    original_name = update_db_button.name
    update_db_button.name = "Updating..."
    
    try:
        qc_db_utils.update_db(db_path=NAIVE_DB_PATH, naive_only=True)
        pn.state.notifications.success("Database updated successfully")
    except Exception as e:
        pn.state.notifications.error(f"Error updating database: {str(e)}")
        logger.exception("Error updating database")
    finally:
        update_db_button.name = original_name
        update_db_button.disabled = False


update_db_button = pn.widgets.Button(
    name="[admin] Re-scan files",
    width=BUTTON_WIDTH,
    button_type="warning",
)
update_db_button.on_click(update_db_callback)


def app():
    sidebar = pn.Column(
        metrics_pane,
        pn.layout.Divider(margin=(20, 0, 15, 0)),
        no_button,
        yes_button,
        unsure_button,
        qc_rating_pane,
        undo_button,
        skip_button,
        shortcuts_component,
        pn.layout.Divider(margin=(20, 0, 15, 0)),
        pn.pane.Markdown("### Filter"),
        plot_name_filter_dropdown,
        qc_rating_filter_radio,
        show_errors_toggle,
        session_id_filter_dropdown,
        session_id_filter_button,
        pn.layout.Divider(margin=(20, 0, 15, 0)),
        update_db_button,
    )

    return pn.template.MaterialTemplate(
        site="DR dashboard",
        title="experiment QC",
        sidebar=[sidebar],
        main=pn.Column(qc_item_pane, chart),
        sidebar_width=SIDEBAR_WIDTH,
    )


app().servable()
