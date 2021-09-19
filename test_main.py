import unittest
from math import factorial
from typing import Any

import main

LITERALS: list[tuple[str, Any]] = [
    ('''[]''', []),
    ('''123''', 123),
    ('''0''', 0),
    ('''-123''', -123),
    ('''0.123991''', 0.123991),
    ('''-0.123991''', -0.123991),
    ('''"abc"''', 'abc'),
    ('''""''', ''),
    ('''true''', True),
    ('''false''', False),
    ('''nil''', None),
]

VAR_NAMES = ('x', 'kyz', 'u_bsz', '_', '_default')

DUMMY_VALUE = 321


class TestInterpreter(unittest.TestCase):

    def test_re_assign(self):
        for code, expected_value in LITERALS:
            with self.assertRaises(BaseException):
                for var_name in VAR_NAMES:
                    main.interpret(f'''
                        {var_name} = {code.strip()}
                        {var_name} = {code.strip()}
                    ''')

    def test_literals(self):
        for code, expected_value in LITERALS:
            self.assertEqual(main.interpret(code), expected_value)
            for code2, expected_value2 in LITERALS:
                self.assertEqual(main.interpret(f'''[
                    {code.strip()}
                    {code2.strip()}
                ]'''), [expected_value, expected_value2])

    def test_comments(self):
        for code, expected_value in LITERALS:
            self.assertEqual(main.interpret('''
            # kkk asdlkfajsfd

            ''' + code), expected_value)

    def test_fn_def(self):
        for code, expected_value in LITERALS:
            fn: main.CustomFn = main.interpret(f'''fn => {code.strip()}''')

            self.assertIsInstance(fn, main.CustomFn)

            self.assertEqual(fn.params, tuple())

    def test_fn_call(self):
        for code, expected_value in LITERALS:
            self.assertEqual(main.interpret(f'''
            f = fn => {code.strip()}
            f<>
            '''), expected_value)

    def test_fn_call_with_arg(self):
        for code, expected_value in LITERALS:
            for var_name in VAR_NAMES:
                self.assertEqual(main.interpret(f'''
                f = fn {var_name} => {var_name}
                f<{code.strip()}>
                '''), expected_value)

    def test_fn_call_with_multiple_args(self):
        for code, expected_value in LITERALS:
            for var_name in VAR_NAMES:
                self.assertEqual(main.interpret(f'''
                f = fn {var_name} {var_name}_{var_name} => {var_name}_{var_name}
                f<{DUMMY_VALUE} {code.strip()}>
                '''), expected_value)

    def test_if_stmt(self):

        for code, expected_value in LITERALS:
            for condition in ('true', 'not<false>'):
                self.assertEqual(main.interpret(f'''
                if {condition}
                   {code.strip()}
                   {DUMMY_VALUE}
                '''), expected_value)

        for code, _ in LITERALS:
            for condition in ('false', 'not<true>'):
                self.assertEqual(main.interpret(f'''
                if {condition}
                   {code.strip()}
                   {DUMMY_VALUE}
                '''), DUMMY_VALUE)

    def test_recursion(self):
        for i in range(10):
            self.assertEqual(main.interpret(f'''
            fact = fn n => match n
                @0 1
                @n mul<n fact<sub<n 1> > >

            fact<{i}>
            '''), factorial(i))

    def test_pattern_matching_literal_equality(self):
        for code, expected_value in LITERALS:
            self.assertEqual(main.interpret(f'''
                match {code.strip()}
                    @{code.strip()} {DUMMY_VALUE}
                '''), DUMMY_VALUE)

    def test_pattern_matching_var_name_pattern_matches_all(self):
        for code, expected_value in LITERALS:
            for var_name in VAR_NAMES:
                self.assertEqual(main.interpret(f'''
                    match {code.strip()}
                        @{var_name} {var_name}
                    '''), expected_value)
                self.assertEqual(main.interpret(f'''
                    match {code.strip()}
                        @{var_name} {DUMMY_VALUE}
                    '''), DUMMY_VALUE)

    def test_pattern_matching_list(self):
        for code, expected_value in LITERALS:
            for var_name in VAR_NAMES:
                self.assertEqual(main.interpret(f'''
                    match [{code.strip()} {DUMMY_VALUE}]
                        @[{var_name} {var_name}_{var_name}] {var_name}_{var_name}
                    '''), DUMMY_VALUE)

    def test_pattern_matching_list_applies_recursively(self):
        for code, expected_value in LITERALS:
            for var_name in VAR_NAMES:
                self.assertEqual(main.interpret(f'''
                    match [[{code.strip()}] {DUMMY_VALUE}]
                        @[[{var_name}] {var_name}_{var_name}] {var_name}
                    '''), expected_value)

    def test_pattern_matching_ellipsis_empty(self):
        self.assertEqual(main.interpret(f'''
         match []
            @[...tail] tail
        '''), [])

    def test_pattern_matching_ellipsis(self):
        self.assertEqual(main.interpret(f'''
        head = fn xs => match xs
            @[fst ...tail] fst

        head<[{DUMMY_VALUE} 2 3 ]>
        '''), DUMMY_VALUE)

        self.assertEqual(main.interpret(f'''
        head = fn xs => match xs
            @[fst [snd ...tail]] tail
        head<[{DUMMY_VALUE} [2 3 4] ]>
        '''), [3, 4])

    def test_stdlib_reduce(self):
        self.assertEqual(main.interpret(f'''
        reduce<add [1 2 3]>
        '''), 6)

    def test_stdlib_map_empty(self):
        self.assertEqual(main.interpret(f'''
        inc = fn x => add<x 1>
        map<inc []>
        '''), [])

    def test_stdlib_map(self):
        self.assertEqual(main.interpret(f'''
        inc = fn x => add<x 1>
        map<inc [1 2 3]>
        '''), [2, 3, 4])

    def test_stdlib_range(self):
        for end in (0, 10, 100):
            self.assertEqual(main.interpret(f'''
            range<0 {end}>
            '''), list(range(0, end)))

    def test_vars(self):
        for code, expected_value in LITERALS:
            for var_name in VAR_NAMES:
                self.assertEqual(main.interpret(f'''
                {var_name} = {code.strip()}
                {var_name}
                '''), expected_value)

    def test_actor_self_fn(self):
        for code, expected_value in LITERALS:
            self.assertEqual(main.interpret(f'''
            ({code.strip()}) >> self<>
            <<
            '''), expected_value)

    def test_actor_bg_task(self):
        for code, expected_value in LITERALS:
            self.assertIsInstance(main.interpret(f'''
                id = fn => {code.strip()}
                process = &id<>
                '''), str)

    def test_actor_await_bg_task(self):
        for code, expected_value in LITERALS:
            self.assertEqual(main.interpret(f'''!&{code.strip()}'''), expected_value)

    def test_actor_send_msg(self):
        for code, expected_value in LITERALS:
            self.assertEqual(main.interpret(f'''
            f = fn => <<

            result = & f<>

            ({code.strip()}) >> result

            !result
            '''), expected_value)

    def test_actor_multi_send(self):
        self.assertEqual(main.interpret('''

        func = fn x => x
        f = fn => map<func [
            <<
            <<
            <<
        ]>

        process = &f<>

        "1" >> "2" >> "3" >> process

        !process
        '''), ["3", "2", "1"])


if __name__ == '__main__':
    unittest.main()
