import os
import openpyxl as opx
import pandas as pd

from openpyxl.styles import Alignment, PatternFill, Font
from openpyxl.styles.borders import Border, Side, BORDER_THIN
from openpyxl.utils import get_column_letter

from utils_ak.color import cast_color


def init_workbook(sheet_names=None, active_sheet_name=None):
    workbook = opx.Workbook()
    sheet_names = sheet_names or []
    for i, sheet_name in enumerate(sheet_names):
        workbook.create_sheet(sheet_name, i)
    workbook.remove(workbook.worksheets[-1])
    if active_sheet_name:
        workbook.active = sheet_names.index(active_sheet_name)
    return workbook


def cast_workbook(wb_obj):
    wb_obj = wb_obj or ['Sheet1']
    if isinstance(wb_obj, str):
        return opx.load_workbook(filename=wb_obj, data_only=True)
    elif isinstance(wb_obj, opx.Workbook):
        return wb_obj
    elif isinstance(wb_obj, list):
        return init_workbook(sheet_names=wb_obj)
    else:
        raise Exception("Unknown workbook format")


def cast_worksheet(ws_obj):
    if isinstance(ws_obj, opx.worksheet.worksheet.Worksheet):
        return ws_obj

    elif isinstance(ws_obj, (tuple, list)):
        wb_obj, sheet_name = ws_obj
        wb = cast_workbook(wb_obj)
        return wb.worksheets[wb.sheetnames.index(sheet_name)]


def set_border(sheet, x, y, w, h, border):
    rows = sheet[
        "{}{}".format(get_column_letter(x), y) : "{}{}".format(
            get_column_letter(x + w - 1), y + h - 1
        )
    ]

    for row in rows:
        row[0].border = Border(
            left=border,
            top=row[0].border.top,
            bottom=row[0].border.bottom,
            right=row[0].border.right,
        )
        row[-1].border = Border(
            left=row[-1].border.left,
            top=row[-1].border.top,
            bottom=row[-1].border.bottom,
            right=border,
        )
    for c in rows[0]:
        c.border = Border(
            left=c.border.left, top=border, bottom=c.border.bottom, right=c.border.right
        )
    for c in rows[-1]:
        c.border = Border(
            left=c.border.left, top=c.border.top, bottom=border, right=c.border.right
        )


def set_border_grid(sheet, x, y, w, h, border):
    for _x in range(x, x + w):
        for _y in range(y, y + h):
            set_border(sheet, _x, _y, 1, 1, border)


def get_sheet_by_name(wb, sheet_name):
    return wb.worksheets[wb.sheetnames.index(sheet_name)]


def set_zoom(sheet, zoom_scale):
    sheet.sheet_view.zoomScale = zoom_scale


def set_dimensions(sheet, orientation, rng, length):
    if orientation == 'column':
        for i in rng:
            sheet.column_dimensions[get_column_letter(i)].width = length
    else:
        for i in rng:
            sheet.row_dimensions[i].height = length


def draw_cell(
    sheet, x, y, text, color=None, font_size=None, text_rotation=None, alignment=None
):
    cell = sheet.cell(row=y, column=x)
    cell.font = Font(size=font_size)
    if alignment == "center":
        cell.alignment = Alignment(
            horizontal="center",
            vertical="center",
            wrap_text=True,
            text_rotation=text_rotation,
        )
    cell.value = text

    if color:
        cell.fill = PatternFill("solid", fgColor=color[1:])
    return cell


def draw_merged_cell(
    sheet,
    x1,
    x2,
    h1,
    h2,
    text,
    color=None,
    border=None,
    text_rotation=None,
    font_size=None,
    alignment=None,
    bold=False,
):
    color = color or cast_color("white")
    sheet.merge_cells(
        start_row=x2, start_column=x1, end_row=x2 + h2 - 1, end_column=x1 + h1 - 1
    )
    merged_cell = sheet.cell(row=x2, column=x1)
    merged_cell.font = Font(size=font_size, bold=bold)
    if alignment == "center":
        merged_cell.alignment = Alignment(
            horizontal="center",
            vertical="center",
            wrap_text=True,
            text_rotation=text_rotation,
        )
    merged_cell.value = text
    if color:
        merged_cell.fill = PatternFill("solid", fgColor=color[1:])

    if border is not None:
        if isinstance(border, dict):
            border = Side(**border)
        elif isinstance(border, Side):
            pass
        else:
            raise Exception("Unknown border type")

        set_border(sheet, x1, x2, h1, h2, border)


def draw_row(sheet, y, values, color=None, **kwargs):
    for i, v in enumerate(values, 1):
        draw_cell(sheet, i, y, text=v, color=color, **kwargs)


def _cast_alpha_hex_to_hex(alpha_hex):
    if str(alpha_hex).lower() == '00000000': # consider transparent black as white (sometimes it is the case for some reason)
        return cast_color('white')
    else:
        return cast_color('#' + alpha_hex[2:])


def read_merged_cells_df(ws_obj, basic_features=True):
    ws = cast_worksheet(ws_obj)
    df = pd.DataFrame()
    df["cell"] = ws.merged_cells.ranges

    bound_names = ("x0", "x1", "y0", "y1")
    df["bounds"] = df["cell"].apply(lambda cell: cell.bounds)
    for i in range(4):
        df[bound_names[i]] = df["bounds"].apply(lambda bound: bound[i])

    df["y0"] += 1
    df["y1"] += 1
    df["label"] = df["cell"].apply(lambda cell: cell.start_cell.value)

    df['font_size'] = df["cell"].apply(lambda cell: cell.start_cell.font.sz)
    df['is_bold'] = df["cell"].apply(lambda cell: cell.start_cell.font.b)
    df['color'] = df["cell"].apply(lambda cell: _cast_alpha_hex_to_hex(cell.start_cell.fill.fgColor.rgb))
    df['text_rotation'] = df["cell"].apply(lambda cell: cell.start_cell.alignment.textRotation)
    df = df.sort_values(by=["x1", "x0", "y1", "y0"])
    if basic_features:
        df = df[['x0', 'x1', 'y0', 'y1', 'label']]

    return df


def draw_merged_cells(ws_obj, merged_cells_df):
    ws = cast_worksheet(ws_obj)
    for i, row in merged_cells_df.iterrows():
        draw_merged_cell(
            ws,
            row['x0'],
            row['x1'],
            row['y0'] - row['x0'],
            row['y1'] - row['x1'],
            row['label'],
            cast_color(row['color']),
            bold=row['is_bold'],
            border={"border_style": "thin", "color": "000000"},
            text_rotation=row['text_rotation'],
            font_size=row['font_size'],
            alignment="center")
    return ws


def draw_sheet_sequence(ws_obj, sheet_objs):
    cur_y_axis_shift = 0
    for sheet_obj in sheet_objs:
        merged_cells_df = read_merged_cells_df(sheet_obj, False)
        height = merged_cells_df['y1'].max()
        merged_cells_df['x1'] += cur_y_axis_shift
        merged_cells_df['y1'] += cur_y_axis_shift
        draw_merged_cells(ws_obj, merged_cells_df)
        cur_y_axis_shift += height


if __name__ == "__main__":
    print(read_merged_cells_df(('sample.xlsx', 'Sheet1'), basic_features=False))
    wb = init_workbook(["a", "b"], active_sheet_name="b")
    set_border_grid(wb.worksheets[0], 1, 1, 10, 10, Side(border_style=BORDER_THIN))

    draw_sheet_sequence((wb, 'b'), (('sample1.xlsx', 'Расписание'), ('sample1.xlsx', 'Расписание')))

    wb.save('output.xlsx')