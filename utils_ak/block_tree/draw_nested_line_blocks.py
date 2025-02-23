import json
import random
from collections import defaultdict
from typing import Literal, Optional

from utils_ak.block_tree import ParallelepipedBlock, BlockMaker
from loguru import logger

from utils_ak.block_tree.validation.class_validator import ClassValidator
from utils_ak.block_tree.validation.validate_disjoint import validate_disjoint
from utils_ak.color import cast_color
from utils_ak.openpyxl import (
    init_workbook,
    cast_worksheet,
    set_active_sheet,
    draw_merged_cell,
    set_zoom,
    set_dimensions,
)
from utils_ak.os import open_file_in_os

random.seed(42)


def draw_nested_line_blocks(block: ParallelepipedBlock, skipped: list[str] = []):
    # - Init excel workbook

    wb = init_workbook(["Default"])
    ws = cast_worksheet((wb, "Default"))
    set_active_sheet(wb, "Default")

    # - Prepare excel workbook

    set_zoom(ws, 55)
    set_dimensions(ws, "column", range(1, 5), 21)
    set_dimensions(ws, "column", range(5, 288 * 2), 2.4)
    set_dimensions(ws, "row", range(1, 220), 25)

    # - Create frontend block

    rectangles_by_y_axis = defaultdict(list)
    colors_by_class = {}

    def add_block(block):
        # - Get rectangle

        rectangle = {
            "x": block.x[0] + 1,
            "y": 1,
            "width": block.size[0],
            "height": 1,
            "text": block.props["cls"],
        }

        # - Get color (random if not in colors_by_class)

        if block.props["cls"] not in colors_by_class:
            colors_by_class[block.props["cls"]] = cast_color(
                (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
            )
        rectangle["color"] = colors_by_class[block.props["cls"]]

        # - Iterate to find the y axis that is free

        found = False
        for y_axis in range(1, 1000):
            rectangle["y"] = y_axis

            level_rectangles = rectangles_by_y_axis[y_axis]

            # - Check if no intersections

            if all(
                max(
                    min(_rectangle["x"] + _rectangle["width"], rectangle["x"] + rectangle["width"])
                    - max(_rectangle["x"], rectangle["x"]),
                    0,
                )
                == 0
                for _rectangle in level_rectangles
            ):
                rectangles_by_y_axis[y_axis].append(rectangle)
                found = True
                break

        # - If not found, raise exception

        if not found:
            raise Exception("No free y axis")

    # - Add blocks

    for block in block.iter():
        # - Skip if needed

        if block.props["cls"] in skipped:
            continue

        # - Add block

        add_block(block)

    # - Draw schedule

    for rectangle in sum(rectangles_by_y_axis.values(), []):
        if rectangle["width"] == 0:
            continue

        draw_merged_cell(
            ws,
            rectangle["x"],
            rectangle["y"],
            rectangle["width"],
            rectangle["height"],
            rectangle["text"],
            color=rectangle["color"],
            bold=True,
            font_size=9,
            alignment="center",
            border={"border_style": "thin", "color": "000000"},
        )

    # - Save file

    wb.save("output.xlsx")

    # - Open file

    open_file_in_os("output.xlsx")


def test():
    a = ParallelepipedBlock("a", n_dims=2, x=[1, 0], size=[3, 0])
    b = ParallelepipedBlock("b", n_dims=2, x=[0, 0], size=[1, 0])
    c = ParallelepipedBlock("c", n_dims=2, x=[1, 0], size=[2, 0])
    a.add_child(b)
    a.add_child(c)
    draw_nested_line_blocks(a)


if __name__ == "__main__":
    test()
