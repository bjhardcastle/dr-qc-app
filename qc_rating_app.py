import datetime
import json
import logging
import time
from typing import NotRequired, TypedDict


import altair as alt
import npc_io
import panel as pn
import param
import polars as pl
import upath
import panel.custom 

import qc_db_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  #! doesn't take effect

# pn.config.theme = 'dark'
pn.config.notifications = True


SIDEBAR_WIDTH = 280
BUTTON_WIDTH = int(SIDEBAR_WIDTH * 0.8)

if not upath.UPath(qc_db_utils.DB_PATH).exists():
    qc_db_utils.create_db()

db_df: pl.DataFrame = qc_db_utils.get_db()

def get_metrics(qc_path: str):
    return (
        qc_db_utils.get_db()
        .filter(pl.col("qc_path") == qc_path)
        .select('session_id', 'qc_group', 'plot_name', 'plot_index')
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

### `{path}`
"""
    for k, v in metrics.items():
        if isinstance(v, float):
            v = f"{v:.2f}"
        stats += f"\n{k.replace('_', ' ')}:\n`{v if v else '-'}`\n"
    return pn.pane.Markdown(stats)


def display_rating(
    path=str,
) -> pn.pane.Markdown:
    df = qc_db_utils.get_db().filter(pl.col('qc_path')==path)
    assert len(df) == 1, f"Expected 1 row, got {len(df)}"
    row = df.to_dicts()[0]
    rating: int | None = row["qc_rating"]
    if rating is None:
        return pn.pane.Markdown("not yet rated")
    else:
        return pn.pane.Markdown(
            f"**{qc_db_utils.QCRating(rating).name.title()}** ({datetime.datetime.fromtimestamp(row['checked_timestamp']).strftime('%Y-%m-%d %H:%M:%S')})"
        )


def display_item(qc_path: str):
    path = upath.UPath(qc_path)
    logger.info(f"Displaying {path}")
    if path.suffix == '.png': 
        return pn.pane.PNG(
            path.read_bytes(),
            sizing_mode="stretch_both",
        )
    if path.suffix == '.json':
        return pn.pane.JSON(
            json.loads(path.read_bytes()),
            sizing_mode="stretch_height",
        )
    if path.suffix in ('.txt', '.error'):
        return pn.pane.Str(
            path.read_text(),
            sizing_mode="stretch_height",
        )
    if path.suffix == '.link':
        link = upath.UPath(path.read_text())
        if link.as_posix().startswith('http'):
            return pn.pane.HTML(
                f'<a href="{link.read_text()}">Link</a>',
                sizing_mode="stretch_height",
            )
        if link.as_posix().startswith('s3://'):
            return pn.pane.Video(
                npc_io.get_presigned_url(link), loop=False, autoplay=True, width=1600
            )

        

# initialize for startup, then update as a global variable
qc_path_generator = qc_db_utils.qc_item_generator(
    already_checked=False,
)
current_qc_path = next(qc_path_generator)
previous_qc_path: str | None = None
metrics_pane = display_metrics(current_qc_path)
qc_rating_pane = display_rating(current_qc_path)
qc_item_pane = display_item(current_qc_path)


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
    qc_item_pane.object = display_item(current_qc_path).object
    metrics_pane.object = display_metrics(current_qc_path).object
    qc_rating_pane.object = display_rating(current_qc_path).object


def update_and_next(path: str, qc_rating: int) -> None:
    qc_db_utils.set_qc_rating_for_path(path=path, qc_rating=qc_rating)
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
yes_button_text = f"{qc_db_utils.QCRating.PASS.name.title()} [{qc_db_utils.QCRating.PASS.value}]"
yes_button = pn.widgets.Button(name=yes_button_text, width=BUTTON_WIDTH)
yes_button.on_click(
    lambda event: update_and_next(
        path=current_qc_path, qc_rating=qc_db_utils.QCRating.PASS
    )
)
unsure_button_text = f"{qc_db_utils.QCRating.UNSURE.name.title()} [{qc_db_utils.QCRating.UNSURE.value}]"
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
        update_and_next(
            path=current_qc_path, qc_rating=qc_db_utils.QCRating[event.data]
        )


shortcuts_component.on_msg(handle_shortcut)


chart = (
    alt.Chart(
        data=(
            db_df
            .group_by('plot_name', 'qc_group')
            .agg(
                pl.col('qc_rating').value_counts(),
            )
            .explode('qc_rating')
            .unnest('qc_rating')
            .sort('qc_group')
            .with_columns(
                pl.col('qc_rating').replace_strict([0, 1, 5, None] , ['fail', 'pass', 'unsure', 'not rated'])
            )
            .with_columns(pl.concat_str(['qc_group', 'plot_name'], separator='/').alias('name'))
        ).to_pandas()
    )
    .mark_bar()
    .encode(
        x='name:N',
        y=alt.Y('count').scale(type="symlog"),
        color='qc_rating:N',
        tooltip=['plot_name', 'qc_group', 'qc_rating', 'count']
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
    path_generator_params = dict(
        qc_rating_filter=qc_rating_filter_radio.value,
        session_id_filter=handle_no_filter(session_id_filter_dropdown.value),
        plot_name_filter=(
            handle_no_filter(plot_name_filter_dropdown.value).split('/')[-1]
            if handle_no_filter(plot_name_filter_dropdown.value) is not None
            else None
        ),        
    )
    print(path_generator_params)
    if qc_rating_filter_radio.value == "unrated":
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
    qc_path_generator = qc_db_utils.qc_item_generator(**path_generator_params)
    next_path()

plot_name_filter_dropdown = pn.widgets.Select(
    name="Plot name",
    value="no filter",
    options=(
        db_df
        .select(pl.concat_str(['qc_group', 'plot_name'], separator='/').alias('name'))
        .get_column('name').unique().sort(descending=True)
        .to_list() + ["no filter"]
    ),
)
plot_name_filter_dropdown.param.watch(update_path_generator, "value")

session_id_filter_dropdown = pn.widgets.Select(
    name="Session ID",
    value="no filter",
    options=db_df["session_id"].unique().sort(descending=False).to_list() + ["no filter"],
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
        session_id_filter_dropdown,
    )

    return pn.template.MaterialTemplate(
        site="DR dashboard",
        title="experiment QC",
        sidebar=[sidebar],
        main=pn.Column(qc_item_pane, chart),
        sidebar_width=SIDEBAR_WIDTH,
    )


app().servable()
