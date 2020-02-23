# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import concurrent.futures.process  # Using long import to simplify testing
import typing

from bravado_core.spec import Spec

from swagger_spec_compatibility.rules.common import RuleProtocol
from swagger_spec_compatibility.rules.common import RuleRegistry
from swagger_spec_compatibility.rules.common import ValidationMessage


class _ALL_RULES(object):
    def __str__(self):
        # type: () -> str
        return str('ALL_RULES')  # pragma: no cover  # This statement is present only to have a nicer REPL experience


def _validate(
    args,  # type: typing.Tuple[typing.Type[RuleProtocol], Spec, Spec]
):
    # type: (...) -> typing.Tuple[typing.Type[RuleProtocol], typing.List[ValidationMessage]]
    rule, old_spec, new_spec = args
    return rule, list(rule.validate(left_spec=old_spec, right_spec=new_spec))


def compatibility_status(
    old_spec,  # type: Spec
    new_spec,  # type: Spec
    rules=_ALL_RULES(),  # type: typing.Union[_ALL_RULES, typing.Iterable[typing.Type[RuleProtocol]]]
    max_parallelism=None,  # type: typing.Optional[int]
):
    # type: (...) -> typing.Mapping[typing.Type[RuleProtocol], typing.Iterable[ValidationMessage]]

    if isinstance(rules, _ALL_RULES):
        rules = RuleRegistry.rules()

    with concurrent.futures.process.ProcessPoolExecutor(max_workers=max_parallelism) as pool:
        rules_to_error_level_mapping = {
            rule: validation_messages
            for rule, validation_messages in pool.map(
                _validate,
                (
                    (rule, old_spec, new_spec)
                    for rule in rules
                ),
            )
        }

    return rules_to_error_level_mapping
