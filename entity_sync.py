# Data Wrangling
import pandas as pd

# Data Parsing
import jmespath as jm

# Local Libraries
from auth import *
from functions import *

# WissKI
from wisski.api import Api, Entity


# Class for Associated Entities Synchronisation
class EntitySync(GeneralEntity):
    """
    PSEUDO-CODE
    - List of mongo entities
    - List of WissKI bundle entities
    - Compare function
    - Check function/Staged function
    - Update function
    """

    def __init__(self, api: Api, sync_field: str):
        # API-client
        self._api = api

        # Field name initialisation
        self._sync_field_name = sync_field
        self._collection = mongodata_fetch(db_name='dev', collection_name=self._sync_field_name)
        self._single_fields = ['institutions', 'groups']
        self._double_fields = ['persons', 'collections']

        # Super Class
        super().__init__()

        # Bundle dictionary
        self._bundle_dict = {
            "institutions": {
                "query": "institutionlist",
                "field": "f_institution_name",
                "group": "g_institution"
            },
            "persons": {
                "query": "personlist",
                "field": "f_person_name",
                "alt_field": "f_person_affiliation",
                "alt_field_label": "affiliation",
                "group": "g_person"
            },
            "groups": {
                "query": "grouplist",
                "field": "f_group_name",
                "group": "g_group"
            },
            "collections": {
                "query": "collectionlist",
                "field": "f_collection_title",
                "alt_field": "f_collection_identifier",
                "alt_field_label": "identifier",
                "group": "g_collection"
            }
        }

    # WissKI Entity List
    def wisski_list(self):
        # Gets the appropriate query from bundle_dict based on sync_field_name
        # Returns a list of all entities currently in WissKI
        return list(entity_uri(
            search_value="",
            query_string=self._query.get(self._bundle_dict.get(self._sync_field_name)['query']),
            return_format='csv',
            value_input=False
        ).iloc[:, 0])

    def missing_entities(self):
        # Gets lists from both systems
        wisskiList = self.wisski_list()   # Entities in WissKI
        mongo_response = self._collection   # Entities in MongoDB
        mongoList = jm.search("[].name", mongo_response)   # Extracts names from MongoDB data
        
        # Finds entities that exist in MongoDB but not in WissKI
        missing = []
        for entity in mongoList:
            if entity not in wisskiList:
                # Gets full entity data from MongoDB for missing entities
                missing.append(jm.search(f"[?name == '{entity}']", mongo_response)[0])
            else:
                pass
        return missing

    def staged(self):
        entity_values_list = []   # Stores the field values
        entity_list = []   # Stores the Entity objects
        field_dict = self._bundle_dict.get(self._sync_field_name)

        # Process each missing entity
        for entity in self.missing_entities():
            # Create basic entity value with name
            entity_value = {
                self._field.get(field_dict.get('field')): [entity.get("name")]
            }
            # Handle entities that have additional fields (persons and collections)
            if self._sync_field_name in self._double_fields:
                if self._sync_field_name == 'persons':
                    # Special handling for person affiliations
                    if entity.get(field_dict.get('alt_field_label')):
                        entity_value[self._field.get(field_dict.get('alt_field'))] = entity_list_generate(
                            value_list=entity.get(field_dict.get('alt_field_label')),
                            query_name=self._query.get('institution')
                        )
                else:
                    # Handle other double fields (like collections)
                    entity_value[self._field.get(field_dict.get('alt_field'))] = [
                        entity.get(field_dict.get('alt_field_label'))
                    ]
            else:
                pass

            # Store the values and create entity object
            entity_values_list.append(entity_value)
            entity_object = Entity(
                api=self._api,
                fields=entity_value,
                bundle_id=self._bundle.get(field_dict.get('group'))
            )
            # Appending entity object list
            entity_list.append(entity_object)

        return {'entities': entity_list, 'values': entity_values_list}

    # Take staged entities and save them to WissKI
    def update(self):
        for entity_obj in self.staged()['entities']:
            self._api.save(entity_obj)

    # Person entity affiliation the synchronise
    """
    In the scenario where a pre-existing person entity
    has new affiliation to be updated. This function must
    check and update entities with new values. (This must be
    done once institution list is up-to-date)
    """

    def affiliations_update(self, single_person: str | None = None):
        """
        Updates person entities with new affiliations from MongoDB.

        Args:
        single_person (str, optional): Name of specific person to update
        Returns:
        str: A summary of the update process including successes and warnings
        """
        # Get persons from MongoDB, filtered by name if provided
        if single_person:
            mongo_persons = [p for p in self._collection if p['name'] == single_person]
        else:
            mongo_persons = self._collection
        
        # If no persons are found, return a message
        if not mongo_persons:
            return f"No person found with name: {single_person}"
        
        updates = []   # List to store entities that need updating
        update_details = []   # List to store update details for logging
        
        for person in mongo_persons:
            try:
                print("Processing person:", person['name'])
                
                # Query WissKI to find the URI of the person using predefined query
                existing_uri = entity_uri(
                    search_value=person['name'],
                    query_string=self._query.get('person'),   # This query already includes {search_value}
                    return_format='csv',
                    value_input=True
                )
                
                print("URI result:", existing_uri if not existing_uri.empty else "No URI found")
                
                # If the person exists in WissKI, proceed with updating
                if not existing_uri.empty:
                    fields = {
                        self._field.get('f_person_name'): [person['name']]
                    }
                    
                    # Handle affiliations
                    affiliations = person.get('affiliation', [])
                    if affiliations:
                        print("Affiliations:", affiliations)
                        
                        # Get URIs for each institution affiliation
                        current_affiliations = []
                        for affiliation in affiliations:
                            print(f"\nSearching for institution: {affiliation}")
                            
                            # Use the institution query from sparql_queries.json
                            institution_uri = entity_uri(
                                search_value=affiliation,
                                query_string=self._query.get('institution'),
                                return_format='csv',
                                value_input=True
                            )
                            
                            print("Institution search result:", 
                                  institution_uri if not institution_uri.empty else "No institution found")
                            
                            # If institution URI is found, add it to the list
                            if not institution_uri.empty:
                                current_affiliations.append(institution_uri.iloc[0, 0])
                                print(f"Found institution URI: {institution_uri.iloc[0, 0]}")
                        
                        # If any affiliations were found, add them to the fields
                        if current_affiliations:
                            fields[self._field.get('f_person_affiliation')] = current_affiliations
                            update_details.append(f"{person['name']} -> {', '.join(affiliations)}")
                        else:
                            # If no affiliations were found, log a warning and show available institutions
                            all_institutions = entity_uri(
                                search_value=None,
                                query_string=self._query.get('institutionlist'),
                                return_format='csv',
                                value_input=False
                            )
                            print("\nAvailable institutions in WissKI:")
                            print(all_institutions)
                            update_details.append(f"Warning: No institution URIs found for {person['name']}")
                    else:
                        update_details.append(f"{person['name']} -> no affiliations")
                    
                    # Create an Entity object for the person with updated fields
                    person_entity = Entity(
                        api=self._api,
                        fields=fields,
                        bundle_id=self._bundle.get('g_person'),
                        uri=existing_uri.iloc[0, 0]
                    )
                    
                    # Add the entity to the updates list
                    updates.append(person_entity)
                else:
                    update_details.append(f"Warning: Could not find existing URI for {person['name']}")
                    
            except Exception as e:
                # Log any errors encountered during processing
                update_details.append(f"Error processing {person['name']}: {str(e)}")
                import traceback
                print("Full error:", traceback.format_exc())
                continue
        
        # Perform updates for each entity in the updates list
        success_count = 0
        for entity in updates:
            try:
                self._api.save(entity)
                success_count += 1
            except Exception as e:
                update_details.append(f"Error saving entity: {str(e)}")
        
        # Return a summary of the update process
        summary = f"Successfully updated {success_count} out of {len(updates)} entities"
        details = "\n".join(update_details)
        return f"{summary}\n{details}"



    # TODO: Related Items
    """
    Update reaserch data items with related items link.
    """