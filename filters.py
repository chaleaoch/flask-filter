#! /usr/bin/python3
# -*- encoding: utf-8 -*-
import operator as pyoperator
import re
from datetime import datetime
from typing import TypeVar

import peewee

T = TypeVar("T")


class FilterBaseField:
    def __init__(self, filter_field_py) -> None:
        self.field_py = filter_field_py

    def parse_value(self, value):
        return value

    def is_support_operator(self, operator):
        return True


class FilterCharField(FilterBaseField):
    pass


class FilterBooleanField(FilterBaseField):
    def parse_value(self, value):
        ret = super().parse_value(value)
        return ret == "true"

    def is_support_operator(self, operator):
        return operator not in [
            ">=",
            "<=",
            ">",
            "<",
            "LIKE",
            "ILIKE",
        ]


class FilterIntegerField(FilterBaseField):
    def is_support_operator(self, operator):
        return operator not in [
            "LIKE",
            "ILIKE",
        ]


class FilterDateTimeField(FilterBaseField):
    FORMATS = [
        "%Y-%m-%dT%H:%M:%S",
    ]

    def parse_value(self, value):
        ret = super().parse_value(value)
        for format in self.FORMATS:
            try:
                return datetime.strptime(ret, format)
            except ValueError:
                pass
        return ret

    def is_support_operator(self, operator):
        return operator not in [
            "LIKE",
            "ILIKE",
        ]


class FilterDefaultField(FilterBaseField):
    pass


# def load_filter_classes() -> list["BaseFilter"]:
def load_filter_classes():
    return [OrderingFilter, DefaultFilter]


class BaseFilter:
    def __init__(self, query):
        self.query = query

    def filter(self, query):
        raise Exception("abstract method")

    def get_query(self):
        return self.query

    def get_valid_field_name_list(self):
        return [field.name for field in self.query.selected_columns]

    def get_field_py(self, field_name):
        for field in self.query.selected_columns:
            if field.name == field_name:
                return field
        raise Exception(f"Field {field_name} not found")


class OrderingFilter(BaseFilter):
    def get_default_ordering_field(self):
        for field in self.query.selected_columns:
            if field.model._meta.primary_key.name == field.name:
                return field

    def filter(self, ordering_param):
        field_name_list = []

        if ordering_param is None:
            self.query = self.query.order_by(self.get_default_ordering_field())
            return self

        if ordering_param:
            ordering_param_list = [param.strip() for param in ordering_param.split(",")]
            field_name_list = self.filter_valid_fields(ordering_param_list)

        if field_name_list:
            for field_name in reversed(field_name_list):
                if field_name.startswith("-"):
                    ordering = self.get_field_py(field_name[1:]).desc()
                else:
                    ordering = self.get_field_py(field_name)
                self.query = self.query.order_by(ordering)
        return self

    def filter_valid_fields(self, ordering_param_list):
        def term_valid(ordering_param):
            if ordering_param.startswith("-"):
                ordering_param = ordering_param[1:]
            return ordering_param in valid_field_name_list

        valid_field_name_list = self.get_valid_field_name_list()
        return [
            ordering_param
            for ordering_param in ordering_param_list
            if term_valid(ordering_param)
        ]


class DefaultFilter(BaseFilter):
    MAP = {
        "==": pyoperator.eq,
        "!=": pyoperator.ne,
        ">=": pyoperator.ge,
        "<=": pyoperator.le,
        ">": pyoperator.gt,
        "<": pyoperator.lt,
        "LIKE": pyoperator.mod,
        "ILIKE": pyoperator.pow,
    }
    FIELD_MAP = {
        peewee.CharField: FilterCharField,
        peewee.IntegerField: FilterIntegerField,
        peewee.BooleanField: FilterBooleanField,
        peewee.DateTimeField: FilterDateTimeField,
    }
    SUPPORT_METHOD = [
        "is_null",
    ]

    def split_operator(self, param_key) -> dict | None:
        operator_pattern = ""
        for key in self.MAP.keys():
            operator_pattern += f"{key}|"
        for key in self.SUPPORT_METHOD:
            operator_pattern += f"{key}|"
        operator_pattern = operator_pattern[:-1]

        regexp = re.search(
            rf"(?P<key>.*)\((?P<operator>{operator_pattern}\))", param_key
        )
        if regexp:
            return regexp.groupdict()
        return None

    def get_filter_field(self, field_py):
        for peewee_field_cls, filter_field_cls in self.FIELD_MAP.items():
            if isinstance(field_py, peewee_field_cls):
                filter_field_py = filter_field_cls(field_py)
                return filter_field_py
        return FilterDefaultField(field_py)

    def filter(self, query_params):
        for param_key, value in query_params.items():
            operator_dict = self.split_operator(param_key)
            if operator_dict is None:
                continue
            field_name = operator_dict["key"]
            operator = operator_dict["operator"]
            if field_name not in self.get_valid_field_name_list():
                continue
            field_py = self.get_field_py(field_name)
            filter_field = self.get_filter_field(field_py)
            if not filter_field.is_support_operator(operator):
                continue
            if operator in self.SUPPORT_METHOD:
                self.query = self.query.where(
                    getattr(filter_field.field_py, operator)(value == "true")
                )
            else:
                self.query = self.query.where(
                    self.MAP[operator](
                        filter_field.field_py, filter_field.parse_value(value)
                    )
                )
        return self
