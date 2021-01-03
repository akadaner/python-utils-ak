import os
import openpyxl as opx
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
    if isinstance(wb_obj, str):
        return opx.load_workbook(filename=wb_obj, data_only=True)
    elif isinstance(wb_obj, opx.Workbook):
        return wb_obj
    elif isinstance(wb_obj, list):
        return init_workbook(sheet_names=wb_obj)
    else:
        raise Exception('Unknown workbook format')

def set_border(sheet, x, y, w, h, border):
    rows = sheet['{}{}'.format(get_column_letter(x), y):'{}{}'.format(get_column_letter(x + w - 1), y + h - 1)]

    for row in rows:
        row[0].border = Border(left=border, top=row[0].border.top, bottom=row[0].border.bottom, right=row[0].border.right)
        row[-1].border = Border(left=row[-1].border.left, top=row[-1].border.top, bottom=row[-1].border.bottom, right=border)
    for c in rows[0]:
        c.border = Border(left=c.border.left, top=border, bottom=c.border.bottom, right=c.border.right)
    for c in rows[-1]:
        c.border = Border(left=c.border.left, top=c.border.top, bottom=border, right=c.border.right)


def draw_cell(sheet, x, y, text, color=None, font_size=None, text_rotation=None, alignment=None):
    cell = sheet.cell(row=y, column=x)
    cell.font = Font(size=font_size)
    if alignment == 'center':
        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True, text_rotation=text_rotation)
    cell.value = text

    if color:
        cell.fill = PatternFill("solid", fgColor=color[1:])
    return cell


def draw_block(sheet, x1, x2, h1, h2, text, color=None, border=None, text_rotation=None, font_size=None, alignment=None, bold=False):
    color = color or cast_color('white')
    sheet.merge_cells(start_row=x2, start_column=x1, end_row=x2 + h2 - 1, end_column=x1 + h1 - 1)
    merged_cell = sheet.cell(row=x2, column=x1)
    merged_cell.font = Font(size=font_size, bold=bold)
    if alignment == 'center':
        merged_cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True, text_rotation=text_rotation)
    merged_cell.value = text
    if color:
        merged_cell.fill = PatternFill("solid", fgColor=color[1:])

    if border is not None:
        if isinstance(border, dict):
            border = Side(**border)
        elif isinstance(border, Side):
            pass
        else:
            raise Exception('Unknown border type')

        set_border(sheet, x1, x2, h1, h2, border)


def draw_row(sheet, y, values, color=None, **kwargs):
    for i, v in enumerate(values, 1):
        draw_cell(sheet, i, y, text=v, color=color, **kwargs)


if __name__ == '__main__':
    wb = init_workbook(['a', 'b'], active_sheet_name='b')
    wb.save('text.xlsx')
