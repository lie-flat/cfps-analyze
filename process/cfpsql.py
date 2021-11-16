import sys
import re
from functools import reduce, partial


def clear_spaces(sline):
    return (word for word in sline if word)


def compose(f, g):
    return lambda x: f(g(x))


class CFPSQLSyntaxError(Exception):
    pass


class CFPSQLBuilder:
    def __init__(self, slines):
        it = enumerate(slines)
        self.region = None
        self.year = None
        self.area = None
        self.type = None
        self.variable = None
        self.virtual_table = None
        self.variable_type = None
        self.target_variable = None
        self.do_visualization = False
        self.visualization_type = None
        words = next(it)[1]
        try:
            if words[0] != 'from' or words[2] != 'to':
                raise CFPSQLSyntaxError('Expected "from <year1> to <year2> in <virtual_table>" on the first line.')
            try:
                self.year = int(words[1]), int(words[3])
            except ValueError:
                raise CFPSQLSyntaxError('Expected <year1> and <year2> to be integers.')
            if self.year[0] > self.year[1]:
                raise CFPSQLSyntaxError('Expected <year1> to be less than <year2>.')
            if words[4] != 'in':
                raise CFPSQLSyntaxError(f'Unexpected token: {words[4]} on the first line.')
            self.virtual_table = words[5]
        except IndexError:
            raise CFPSQLSyntaxError('Not enough tokens on the first line.')

        while index_and_words := next(it):
            index, words = index_and_words
            # match words:
            #     case 

    def run(self):
        return None


def parse_cfpsql(lines):
    slines = map(compose(clear_spaces, partial(re.split, '\s+')), lines)
    return CFPSQLBuilder(slines)


def begin_cfpsql_session():
    lines = [input('cfpsql>> ')]
    terminator_cnt = 0
    while line := input('cfpsql.. '):
        if line:
            lines.append(line)
        else:
            terminator_cnt += 1
        if terminator_cnt == 2:
            break
    return parse_cfpsql(lines).run()
