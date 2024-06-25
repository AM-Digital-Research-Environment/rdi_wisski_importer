# Libraries
from functions import json_file
from auth import GeneralEntity

# WissKi Api
from wisski.api import Api, Pathbuilder, Entity

# Exception functions for wherein new values must be added to the system
# Exception functions to be written for fields containing controlled vocabs


class fieldfunction(GeneralEntity):

    def __init__(self, field_name):

        # Super Class
        super().__init__()

        # Field Dictionary
        self._path_dict = {
            'language': {
                'bundle': self._bundle.get('g_iso_language'),
                'field': self._field.get('f_iso_language_identifier')
            },
            'subregion': {
                'bundle': self._bundle.get('g_subregion'),
                'field': self._field.get('f_subregion_name'),
                'qualifier': self._field.get('f_subregion_region')
            },
            'genre': {
                'bundle': self._bundle.get('b_authority_tag'),
                'field': self._field.get('f_auth_tag_tag'),
                'qualifier': self._field.get('f_auth_tag_source')
            },
            'subject': {
                'bundle': self._bundle.get('g_subject'),
                'field': self._field.get('f_subject_tag')
            },
            'tags': {
                'bundle': self._bundle.get('g_tag'),
                'field': self._field.get('f_tag_name')
            },
            'sponsor': {
                'bundle': self._bundle.get('g_funding_body'),
                'field': self._field.get('f_funding_body_name')
            },
            'place': {
                'bundle': self._bundle.get('g_place'),
                'field': self._field.get('f_place_name')
            }
        }

        # Field/Bundle
        self._field_info = self._path_dict.get(field_name)

    # Generalised Exception Function
    def exception(self, entity_value, qualifier_value=None, with_qualifier=False):
        if entity_value is not None:
            fields_data = {self._field_info.get('field'): [entity_value]}
            if with_qualifier:
                fields_data[self._field_info.get('qualifier')] = [qualifier_value]
            else:
                pass
            return Entity(api=self._api,
                          fields=fields_data,
                          bundle_id=self._field_info.get('bundle'))
        else:
            pass
