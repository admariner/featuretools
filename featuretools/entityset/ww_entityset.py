import copy
import logging
import warnings
from collections import defaultdict

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pandas.api.types as pdtypes
from pandas.api.types import is_dtype_equal, is_numeric_dtype

import featuretools.variable_types.variable as vtypes
from featuretools.entityset import deserialize, serialize
from featuretools.entityset.entity import Entity
from featuretools.entityset.relationship import Relationship, RelationshipPath
from featuretools.utils.gen_utils import import_or_none, is_instance
from featuretools.utils.plot_utils import (
    check_graphviz,
    get_graphviz_format,
    save_graph
)
from featuretools.utils.wrangle import _check_time_type, _check_timedelta

ks = import_or_none('databricks.koalas')

pd.options.mode.chained_assignment = None  # default='warn'
logger = logging.getLogger('featuretools.entityset')


class EntitySet(object):
    """
    Stores all actual data for a entityset

    Attributes:
        id
        dataframe_dict
        relationships
        time_type

    Properties:
        metadata

    """

    def __init__(self, id=None, dataframes=None, relationships=None):
        """Creates EntitySet

            Args:
                id (str) : Unique identifier to associate with this instance

                dataframes (dict[str -> tuple(pd.DataFrame, str, str, dict[str -> Variable])]): dictionary of
                    entities. Entries take the format
                    {entity id -> (dataframe, id column, (time_index), (variable_types), (make_index))}.
                    Note that time_index, variable_types and make_index are optional.

                relationships (list[(str, str, str, str)]): List of relationships
                    between entities. List items are a tuple with the format
                    (parent entity id, parent variable, child entity id, child variable).

            Example:

                .. code-block:: python

                    entities = {
                        "cards" : (card_df, "id"),
                        "transactions" : (transactions_df, "id", "transaction_time")
                    }

                    relationships = [("cards", "id", "transactions", "card_id")]

                    ft.EntitySet("my-entity-set", entities, relationships)
        """
        self.id = id
        self.dataframe_dict = {}
        self.relationships = []
        self.time_type = None

        dataframes = dataframes or {}
        relationships = relationships or []
        for df_name in dataframes:
            df = dataframes[df_name][0]

            index_column = None
            time_index = None
            variable_types = None
            make_index = None
            semantic_tags = None
            logical_types = None
            if len(dataframes[df_name]) > 1:
                index_column = dataframes[df_name][1]
            if len(dataframes[df_name]) > 2:
                time_index = dataframes[df_name][2]
            if len(dataframes[df_name]) > 3:
                logical_types = dataframes[df_name][3]
            if len(dataframes[df_name]) > 4:
                semantic_tags = dataframes[df_name][4]
            if len(dataframes[df_name]) > 5:
                make_index = dataframes[df_name][5]
            # --> need to find a better way to pass ww params and if we want to allow other formats like ColumnSchema or TableSchema??
            self.add_dataframe(dataframe_id=df_name,
                               dataframe=df,
                               index=index_column,
                               time_index=time_index,
                               logical_types=logical_types,
                               semantic_tags=semantic_tags,
                               make_index=make_index)

        for relationship in relationships:
            parent_df, parent_column, child_df, child_column = relationship
            self.add_relationship(parent_df, parent_column, child_df, child_column)

        self.reset_data_description()

    def __sizeof__(self):
        return sum([entity.__sizeof__() for entity in self.dataframes])

    def __dask_tokenize__(self):
        return (EntitySet, serialize.entityset_to_description(self.metadata))

    def __eq__(self, other, deep=False):
        if len(self.dataframe_dict) != len(other.dataframe_dict):
            return False
        for eid, e in self.dataframe_dict.items():
            if eid not in other.dataframe_dict:
                return False
            if not e.__eq__(other[eid], deep=deep):
                return False
        for r in self.relationships:
            if r not in other.relationships:
                return False
        return True

    def __ne__(self, other, deep=False):
        return not self.__eq__(other, deep=deep)

    def __getitem__(self, dataframe_id):
        """Get entity instance from entityset

        Args:
            entity_id (str): Id of entity.

        Returns:
            :class:`.Entity` : Instance of entity. None if entity doesn't
                exist.
        """
        if dataframe_id in self.dataframe_dict:
            # --> might want to consider returning the accessor or maybe just the schema instead of the DataFrame???
            return self.dataframe_dict[dataframe_id]
        name = self.id or "entity set"
        raise KeyError('DataFrame %s does not exist in %s' % (dataframe_id, name))

    @property
    def dataframes(self):
        return list(self.dataframe_dict.values())

    @property
    def metadata(self):
        '''Returns the metadata for this EntitySet. The metadata will be recomputed if it does not exist.'''
        if self._data_description is None:
            description = serialize.entityset_to_description(self)
            self._data_description = deserialize.description_to_entityset(description)

        return self._data_description

    def reset_data_description(self):
        self._data_description = None

# --> add back when updating serialization for Woodwork
    # def to_pickle(self, path, compression=None, profile_name=None):
    #     '''Write entityset in the pickle format, location specified by `path`.
    #         Path could be a local path or a S3 path.
    #         If writing to S3 a tar archive of files will be written.

    #         Args:
    #             path (str): location on disk to write to (will be created as a directory)
    #             compression (str) : Name of the compression to use. Possible values are: {'gzip', 'bz2', 'zip', 'xz', None}.
    #             profile_name (str) : Name of AWS profile to use, False to use an anonymous profile, or None.
    #     '''
    #     serialize.write_data_description(self, path, format='pickle', compression=compression, profile_name=profile_name)
    #     return self

    # def to_parquet(self, path, engine='auto', compression=None, profile_name=None):
    #     '''Write entityset to disk in the parquet format, location specified by `path`.
    #         Path could be a local path or a S3 path.
    #         If writing to S3 a tar archive of files will be written.

    #         Args:
    #             path (str): location on disk to write to (will be created as a directory)
    #             engine (str) : Name of the engine to use. Possible values are: {'auto', 'pyarrow', 'fastparquet'}.
    #             compression (str) : Name of the compression to use. Possible values are: {'snappy', 'gzip', 'brotli', None}.
    #             profile_name (str) : Name of AWS profile to use, False to use an anonymous profile, or None.
    #     '''
    #     serialize.write_data_description(self, path, format='parquet', engine=engine, compression=compression, profile_name=profile_name)
    #     return self

    # def to_csv(self, path, sep=',', encoding='utf-8', engine='python', compression=None, profile_name=None):
    #     '''Write entityset to disk in the csv format, location specified by `path`.
    #         Path could be a local path or a S3 path.
    #         If writing to S3 a tar archive of files will be written.

    #         Args:
    #             path (str) : Location on disk to write to (will be created as a directory)
    #             sep (str) : String of length 1. Field delimiter for the output file.
    #             encoding (str) : A string representing the encoding to use in the output file, defaults to 'utf-8'.
    #             engine (str) : Name of the engine to use. Possible values are: {'c', 'python'}.
    #             compression (str) : Name of the compression to use. Possible values are: {'gzip', 'bz2', 'zip', 'xz', None}.
    #             profile_name (str) : Name of AWS profile to use, False to use an anonymous profile, or None.
    #     '''
    #     if is_instance(self.dataframes[0].df, ks, 'DataFrame'):
    #         compression = str(compression)
    #     serialize.write_data_description(self, path, format='csv', index=False, sep=sep, encoding=encoding, engine=engine, compression=compression, profile_name=profile_name)
    #     return self

    # def to_dictionary(self):
    #     return serialize.entityset_to_description(self)

    ###########################################################################
    #   Public getter/setter methods  #########################################
    ###########################################################################

    def __repr__(self):
        repr_out = u"Entityset: {}\n".format(self.id)
        repr_out += u"  Entities:"
        for df in self.dataframes:
            if df.shape:
                repr_out += u"\n    {} [Rows: {}, Columns: {}]".format(
                    df.ww.name, df.shape[0], df.shape[1])
            else:
                repr_out += u"\n    {} [Rows: None, Columns: None]".format(
                    df.ww.name)
        repr_out += "\n  Relationships:"

        if len(self.relationships) == 0:
            repr_out += u"\n    No relationships"

        for r in self.relationships:
            repr_out += u"\n    %s.%s -> %s.%s" % \
                (r._child_dataframe_id, r._child_column_id,
                 r._parent_dataframe_id, r._parent_column_id)

        return repr_out

    def add_relationships(self, relationships):
        """Add multiple new relationships to a entityset

        Args:
            relationships (list[tuple(str, str, str, str)] or list[Relationship]) : List of
                new relationships to add. Relationships are specified either as a :class:`.Relationship`
                object or a four element tuple identifying the parent and child columns:
                (parent_dataframe_id, parent_column_id, child_dataframe_id, child_column_id)
        """
        for rel in relationships:
            if isinstance(rel, Relationship):
                self.add_relationship(relationship=rel)
            else:
                self.add_relationship(*rel)
        return self

    def add_relationship(self,
                         parent_dataframe_id=None,
                         parent_column_id=None,
                         child_dataframe_id=None,
                         child_column_id=None,
                         relationship=None):
        """Add a new relationship between entities in the entityset. Relationships can be specified
        by passing dataframe and columns ids or by passing a :class:`.Relationship` object.

        Args:
            parent_dataframe_id (str): Name of the parent dataframe in the EntitySet. Must be specified
                if relationship is not.
            parent_column_id (str): Name of the parent column. Must be specified if relationship is not.
            child_dataframe_id (str): Name of the child dataframe in the EntitySet. Must be specified
                if relationship is not.
            child_column_id (str): Name of the child column. Must be specified if relationship is not.
            relationship (Relationship): Instance of new relationship to be added. Must be specified
                if dataframe and column ids are not supplied.
        """
        if relationship and (parent_dataframe_id or parent_column_id or child_dataframe_id or child_column_id):
            raise ValueError("Cannot specify dataframe and column id values and also supply a Relationship")

        if not relationship:
            relationship = Relationship(self,
                                        parent_dataframe_id,
                                        parent_column_id,
                                        child_dataframe_id,
                                        child_column_id)
        if relationship in self.relationships:
            warnings.warn(
                "Not adding duplicate relationship: " + str(relationship))
            return self

        # _operations?

        # this is a new pair of entities
        child_df = relationship.child_dataframe
        # --> is this a variable??? how do you have id?
        child_column = relationship.child_column.name
        if child_df.ww.index == child_column:
            msg = "Unable to add relationship because child column '{}' in '{}' is also its index"
            raise ValueError(msg.format(child_column, child_df.ww.name))
        parent_df = relationship.parent_dataframe
        parent_column = relationship.parent_column.name
        # --> foreign key tag corresponds to a Id variable?
        if 'foreign_key' not in child_df.ww.semantic_tags[child_column]:
            child_df.ww.add_semantic_tags({child_column: 'foreign_key'})

        # --> what happens if there's another index set?????
        if parent_df.ww.index != parent_column:
            # --> previously had 'convert_data=False` does this means we shouldn't set the underlying index?????
            parent_df.ww.set_index(parent_column)
        # Empty dataframes (as a result of accessing Entity.metadata)
        # default to object dtypes for discrete variables, but
        # indexes/ids default to ints. In this case, we convert
        # the empty column's type to int
        if isinstance(child_df, pd.DataFrame) and \
                (child_df.empty and child_df[child_column].dtype == object and
                 parent_df.ww[parent_column].is_numeric):
            # --> should this match Integer or IntegerNullable????
            child_df[child_column] = pd.Series(name=child_column, dtype=np.int64)

        parent_dtype = parent_df[parent_column].dtype
        child_dtype = child_df[child_column].dtype
        msg = u"Unable to add relationship because {} in {} is Pandas dtype {}"\
            u" and {} in {} is Pandas dtype {}."
        if not is_dtype_equal(parent_dtype, child_dtype):
            raise ValueError(msg.format(parent_column, parent_df.ww.name, parent_dtype,
                                        child_column, child_df.ww.name, child_dtype))

        self.relationships.append(relationship)
        self.reset_data_description()
        return self

    def set_secondary_time_index(self, dataframe, secondary_time_index):
        """Updates metadata to include the secondary time index information"""
        self._check_secondary_time_index(dataframe, secondary_time_index)
        dataframe.ww.metadata['secondary_time_index'] = secondary_time_index

    ###########################################################################
    #   Relationship access/helper methods  ###################################
    ###########################################################################

    def find_forward_paths(self, start_dataframe_id, goal_dataframe_id):
        """
        Generator which yields all forward paths between a start and goal
        entity. Does not include paths which contain cycles.

        Args:
            start_entity_id (str) : id of entity to start the search from
            goal_entity_id  (str) : if of entity to find forward path to

        See Also:
            :func:`BaseEntitySet.find_backward_paths`
        """
        for sub_dataframe_id, path in self._forward_entity_paths(start_dataframe_id):
            if sub_dataframe_id == goal_dataframe_id:
                yield path

    def find_backward_paths(self, start_dataframe_id, goal_dataframe_id):
        """
        Generator which yields all backward paths between a start and goal
        entity. Does not include paths which contain cycles.

        Args:
            start_entity_id (str) : Id of entity to start the search from.
            goal_entity_id  (str) : Id of entity to find backward path to.

        See Also:
            :func:`BaseEntitySet.find_forward_paths`
        """
        for path in self.find_forward_paths(goal_dataframe_id, start_dataframe_id):
            # Reverse path
            yield path[::-1]

    def _forward_entity_paths(self, start_dataframe_id, seen_dataframes=None):
        """
        Generator which yields the ids of all entities connected through forward
        relationships, and the path taken to each. An entity will be yielded
        multiple times if there are multiple paths to it.

        Implemented using depth first search.
        """
        if seen_dataframes is None:
            seen_dataframes = set()

        if start_dataframe_id in seen_dataframes:
            return

        seen_dataframes.add(start_dataframe_id)

        yield start_dataframe_id, []

        for relationship in self.get_forward_relationships(start_dataframe_id):
            next_dataframe = relationship.parent_dataframe.ww.id
            # Copy seen entities for each next node to allow multiple paths (but
            # not cycles).
            descendants = self._forward_entity_paths(next_dataframe, seen_dataframes.copy())
            for sub_dataframe_id, sub_path in descendants:
                yield sub_dataframe_id, [relationship] + sub_path

    def get_forward_entities(self, entity_id, deep=False):
        """
        Get entities that are in a forward relationship with entity

        Args:
            entity_id (str): Id entity of entity to search from.
            deep (bool): if True, recursively find forward entities.

        Yields a tuple of (descendent_id, path from entity_id to descendant).
        """
        for relationship in self.get_forward_relationships(entity_id):
            parent_dataframe_id = relationship.parent_dataframe.ww.name
            direct_path = RelationshipPath([(True, relationship)])
            yield parent_dataframe_id, direct_path

            if deep:
                sub_dataframes = self.get_forward_entities(parent_dataframe_id, deep=True)
                for sub_dataframe_id, path in sub_dataframes:
                    yield sub_dataframe_id, direct_path + path

    def get_backward_entities(self, entity_id, deep=False):
        """
        Get entities that are in a backward relationship with entity

        Args:
            entity_id (str): Id entity of entity to search from.
            deep (bool): if True, recursively find backward entities.

        Yields a tuple of (descendent_id, path from entity_id to descendant).
        """
        for relationship in self.get_backward_relationships(entity_id):
            child_dataframe_id = relationship.child_dataframe.ww.id
            direct_path = RelationshipPath([(False, relationship)])
            yield child_dataframe_id, direct_path

            if deep:
                sub_entities = self.get_backward_entities(child_dataframe_id, deep=True)
                for sub_dataframe_id, path in sub_entities:
                    yield sub_dataframe_id, direct_path + path

    def get_forward_relationships(self, entity_id):
        """Get relationships where entity "entity_id" is the child

        Args:
            entity_id (str): Id of entity to get relationships for.

        Returns:
            list[:class:`.Relationship`]: List of forward relationships.
        """
        return [r for r in self.relationships if r.child_dataframe.ww.name == entity_id]

    def get_backward_relationships(self, entity_id):
        """
        get relationships where entity "entity_id" is the parent.

        Args:
            entity_id (str): Id of entity to get relationships for.

        Returns:
            list[:class:`.Relationship`]: list of backward relationships
        """
        return [r for r in self.relationships if r.parent_dataframe.ww.name == entity_id]

    def has_unique_forward_path(self, start_dataframe_id, end_dataframe_id):
        """
        Is the forward path from start to end unique?

        This will raise if there is no such path.
        """
        paths = self.find_forward_paths(start_dataframe_id, end_dataframe_id)

        next(paths)
        second_path = next(paths, None)

        return not second_path

    ###########################################################################
    #  Entity creation methods  ##############################################
    ###########################################################################

    def add_dataframe(self,
                      dataframe_id,
                      dataframe,
                      index=None,
                      logical_types=None,
                      semantic_tags=None,
                      make_index=False,
                      time_index=None,
                      secondary_time_index=None,
                      already_sorted=False):
        # --> 1. necessary for replacing - rename to add_dataframe - maybe allow other kwargs for Woodwork Table???
        """
        Load the data for a specified entity from a Pandas DataFrame.

        Args:
            entity_id (str) : Unique id to associate with this entity.

            dataframe (pandas.DataFrame) : Dataframe containing the data.

            index (str, optional): Name of the variable used to index the entity.
                If None, take the first column.

            variable_types (dict[str -> Variable/str], optional):
                Keys are of variable ids and values are variable types or type_strings. Used to to
                initialize an entity's store.

            make_index (bool, optional) : If True, assume index does not
                exist as a column in dataframe, and create a new column of that name
                using integers. Otherwise, assume index exists.

            time_index (str, optional): Name of the variable containing
                time data. Type must be in :class:`variables.DateTime` or be
                able to be cast to datetime (e.g. str, float, or numeric.)

            secondary_time_index (dict[str -> Variable]): Name of variable
                containing time data to use a second time index for the entity.

            already_sorted (bool, optional) : If True, assumes that input dataframe
                is already sorted by time. Defaults to False.

        Notes:

            Will infer variable types from Pandas dtype

        Example:
            .. ipython:: python

                import featuretools as ft
                import pandas as pd
                transactions_df = pd.DataFrame({"id": [1, 2, 3, 4, 5, 6],
                                                "session_id": [1, 2, 1, 3, 4, 5],
                                                "amount": [100.40, 20.63, 33.32, 13.12, 67.22, 1.00],
                                                "transaction_time": pd.date_range(start="10:00", periods=6, freq="10s"),
                                                "fraud": [True, False, True, False, True, True]})
                es = ft.EntitySet("example")
                es.entity_from_dataframe(entity_id="transactions",
                                         index="id",
                                         time_index="transaction_time",
                                         dataframe=transactions_df)

                es["transactions"]
                es["transactions"].df

        """
        logical_types = logical_types or {}
        semantic_tags = semantic_tags or {}

# --> not necessary bc handled by woodwork
        # if time_index is not None and time_index == index:
        #     raise ValueError("time_index and index cannot be the same value, %s" % (time_index))

        # if time_index is None:
        #     for variable, variable_type in variable_types.items():
        #         if variable_type == vtypes.DatetimeTimeIndex:
        #             raise ValueError("DatetimeTimeIndex variable %s must be set using time_index parameter" % (variable))

        if len(self.dataframes) > 0:
            if not isinstance(dataframe, type(self.dataframes[0])):
                raise ValueError("All entity dataframes must be of the same type. "
                                 "Cannot add entity of type {} to an entityset with existing entities "
                                 "of type {}".format(type(dataframe), type(self.dataframes[0].df)))

        if dataframe.ww.schema is None:
            # init woodwork with params
            # --> need to handle secondary time index!!!!!!!
            # --> all these arams will be ignored if schema is not none - do we want to either raise warning or handle differenctly?
            dataframe.ww.init(name=dataframe_id,
                              index=index,
                              time_index=time_index,
                              logical_types=logical_types,
                              semantic_tags=semantic_tags,
                              make_index=make_index,
                              already_sorted=already_sorted)

        else:
            # make sure name is set to match input
            dataframe.ww.name = dataframe_id

        if secondary_time_index:
            self.set_secondary_time_index(dataframe, secondary_time_index=secondary_time_index)

        if dataframe.ww.time_index is not None:
            self._check_uniform_time_index(dataframe)
            self._check_secondary_time_index(dataframe)

        # --> need to confirm that the names/dataframe_id match or deal with different names
        self.dataframe_dict[dataframe_id] = dataframe
        self.reset_data_description()
        return self

    def normalize_dataframe(self, base_dataframe_id, new_dataframe_id, index,
                            additional_columns=None, copy_columns=None,
                            make_time_index=None,
                            make_secondary_time_index=None,
                            new_dataframe_time_index=None,
                            new_dataframe_secondary_time_index=None):
        # --> 3. necessary for Woodwork
        """Create a new entity and relationship from unique values of an existing variable.

        Args:
            base_dataframe_id (str) : Entity id from which to split.

            new_dataframe_id (str): Id of the new entity.

            index (str): Variable in old entity
                that will become index of new entity. Relationship
                will be created across this variable.

            additional_columns (list[str]):
                List of variable ids to remove from
                base_entity and move to new entity.

            copy_columns (list[str]): List of
                variable ids to copy from old entity
                and move to new entity.

            make_time_index (bool or str, optional): Create time index for new entity based
                on time index in base_entity, optionally specifying which variable in base_entity
                to use for time_index. If specified as True without a specific variable,
                uses the primary time index. Defaults to True if base entity has a time index.

            make_secondary_time_index (dict[str -> list[str]], optional): Create a secondary time index
                from key. Values of dictionary
                are the variables to associate with the secondary time index. Only one
                secondary time index is allowed. If None, only associate the time index.

            new_dataframe_time_index (str, optional): Rename new entity time index.

            new_dataframe_secondary_time_index (str, optional): Rename new entity secondary time index.

        """
        base_dataframe = self.dataframe_dict[base_dataframe_id]
        additional_columns = additional_columns or []
        copy_columns = copy_columns or []

        # Check base entity to make sure time index is valid
        # --> not sure if still relevalt with woodwork??
        # if base_dataframe.ww.time_index is not None:
        #     t_index = base_dataframe[base_dataframe.ww.time_index]
        #     if not isinstance(t_index, (vtypes.NumericTimeIndex, vtypes.DatetimeTimeIndex)):
        #         base_error = "Time index '{0}' is not a NumericTimeIndex or DatetimeTimeIndex, but type {1}. Use set_time_index on entity '{2}' to set the time_index."
        #         raise TypeError(base_error.format(base_dataframe.time_index, type(t_index), str(base_dataframe.id)))

        if not isinstance(additional_columns, list):
            raise TypeError("'additional_columns' must be a list, but received type {}"
                            .format(type(additional_columns)))

        if len(additional_columns) != len(set(additional_columns)):
            raise ValueError("'additional_columns' contains duplicate variables. All variables must be unique.")

        if not isinstance(copy_columns, list):
            raise TypeError("'copy_columns' must be a list, but received type {}"
                            .format(type(copy_columns)))

        if len(copy_columns) != len(set(copy_columns)):
            raise ValueError("'copy_columns' contains duplicate variables. All variables must be unique.")

        for v in additional_columns + copy_columns:
            if v == index:
                raise ValueError("Not copying {} as both index and variable".format(v))

        for v in additional_columns:
            if v == base_dataframe.ww.time_index:
                raise ValueError("Not moving {} as it is the base time index variable. Perhaps, move the variable to the copy_columns.".format(v))

        if isinstance(make_time_index, str):
            if make_time_index not in base_dataframe.columns:
                raise ValueError("'make_time_index' must be a variable in the base entity")
            elif make_time_index not in additional_columns + copy_columns:
                raise ValueError("'make_time_index' must be specified in 'additional_columns' or 'copy_columns'")
        if index == base_dataframe.ww.index:
            raise ValueError("'index' must be different from the index column of the base entity")

        transfer_types = {}
        # Types will be a tuple of (logical_type, semantic_tags)
        transfer_types[index] = (base_dataframe.ww.logical_types[index], base_dataframe.ww.semantic_tags[index])
        for col_name in additional_columns + copy_columns:
            # Remove any existing time index tags
            transfer_types[col_name] = (base_dataframe.ww.logical_types[col_name], base_dataframe.ww.semantic_tags[col_name] - {'time_index'})

        # create and add new entity
        new_dataframe = self[base_dataframe_id].ww.copy()

        if make_time_index is None and base_dataframe.time_index is not None:
            make_time_index = True

        if isinstance(make_time_index, str):
            # Set the new time index to make_time_index.
            base_time_index = make_time_index
            new_dataframe_time_index = make_time_index
            already_sorted = (new_dataframe_time_index == base_dataframe.time_index)
        elif make_time_index:
            # Create a new time index based on the base entity time index.
            base_time_index = base_dataframe.ww.time_index
            if new_dataframe_time_index is None:
                new_dataframe_time_index = "first_%s_time" % (base_dataframe.ww.name)

            already_sorted = True

            assert base_dataframe.ww.time_index is not None, \
                "Base entity doesn't have time_index defined"

            if base_time_index not in [v for v in additional_columns]:
                copy_columns.append(base_time_index)

            transfer_types[new_dataframe_time_index] = (base_dataframe.ww.logical_types[base_dataframe.ww.time_index], base_dataframe.ww.semantic_tags[base_dataframe.ww.time_index])

        else:
            new_dataframe_time_index = None
            already_sorted = False

        if new_dataframe_time_index is not None and new_dataframe_time_index == index:
            raise ValueError("time_index and index cannot be the same value, %s" % (new_dataframe_time_index))

        selected_columns = [index] +\
            [v for v in additional_columns] +\
            [v for v in copy_columns]

        new_dataframe2 = new_dataframe. \
            drop_duplicates(index, keep='first')[selected_columns]

        if make_time_index:
            new_dataframe2 = new_dataframe2.rename(columns={base_time_index: new_dataframe_time_index})
        if make_secondary_time_index:
            assert len(make_secondary_time_index) == 1, "Can only provide 1 secondary time index"
            secondary_time_index = list(make_secondary_time_index.keys())[0]

            secondary_columns = [index, secondary_time_index] + list(make_secondary_time_index.values())[0]
            secondary_df = new_dataframe. \
                drop_duplicates(index, keep='last')[secondary_columns]
            if new_dataframe_secondary_time_index:
                secondary_df = secondary_df.rename(columns={secondary_time_index: new_dataframe_secondary_time_index})
                secondary_time_index = new_dataframe_secondary_time_index
            else:
                new_dataframe_secondary_time_index = secondary_time_index
            secondary_df = secondary_df.set_index(index)
            new_dataframe = new_dataframe2.join(secondary_df, on=index)
        else:
            new_dataframe = new_dataframe2

        base_dataframe_index = index

        # --> not sure that this is the same as using vtype Categorical because we can't just set a standard tag
        # --> why did we set it to variable Categorical???
        # --> and why does it get reset when we did it above??
        # transfer_types[index] = ('Categorical', set())
        if make_secondary_time_index:
            old_ti_name = list(make_secondary_time_index.keys())[0]
            ti_cols = list(make_secondary_time_index.values())[0]
            ti_cols = [c if c != old_ti_name else secondary_time_index for c in ti_cols]
            make_secondary_time_index = {secondary_time_index: ti_cols}

        if is_instance(new_dataframe, ks, 'DataFrame'):
            already_sorted = False

        # will initialize Woodwork on this DataFrame
        self.add_dataframe(
            new_dataframe_id,
            new_dataframe,
            index,
            already_sorted=already_sorted,
            time_index=new_dataframe_time_index,
            secondary_time_index=make_secondary_time_index,
            logical_types={col_name: logical_type for (col_name, (logical_type, _)) in transfer_types.items()},
            semantic_tags={col_name: (semantic_tags - {'time_index'})for (col_name, (_, semantic_tags)) in transfer_types.items()}
        )

        self.dataframe_dict[base_dataframe_id] = self.dataframe_dict[base_dataframe_id].ww.drop(additional_columns)

        new_dataframe = self.dataframe_dict[new_dataframe_id]
        self.dataframe_dict[base_dataframe_id].ww.add_semantic_tags({base_dataframe_index: 'foreign_key'})

        self.add_relationship(new_dataframe.ww.name, index, base_dataframe.ww.name, base_dataframe_index)
        self.reset_data_description()
        return self

    # ###########################################################################
    # #  Data wrangling methods  ###############################################
    # ###########################################################################

    # def concat(self, other, inplace=False):
    #     '''Combine entityset with another to create a new entityset with the
    #     combined data of both entitysets.
    #     '''
    #     assert_string = "Entitysets must have the same entities, relationships"\
    #         ", and variable_ids"
    #     assert (self.__eq__(other) and
    #             self.relationships == other.relationships), assert_string

    #     for entity in self.dataframes:
    #         assert entity.id in other.dataframe_dict, assert_string
    #         assert (len(self[entity.id].variables) ==
    #                 len(other[entity.id].variables)), assert_string
    #         other_variable_ids = [o_variable.id for o_variable in
    #                               other[entity.id].variables]
    #         assert (all([variable.id in other_variable_ids
    #                      for variable in self[entity.id].variables])), assert_string

    #     if inplace:
    #         combined_es = self
    #     else:
    #         combined_es = copy.deepcopy(self)

    #     has_last_time_index = []
    #     for entity in self.dataframes:
    #         self_df = entity.df
    #         other_df = other[entity.id].df
    #         combined_df = pd.concat([self_df, other_df])
    #         if entity.created_index == entity.index:
    #             columns = [col for col in combined_df.columns if
    #                        col != entity.index or col != entity.time_index]
    #         else:
    #             columns = [entity.index]
    #         combined_df.drop_duplicates(columns, inplace=True)

    #         if entity.time_index:
    #             combined_df.sort_values([entity.time_index, entity.index], inplace=True)
    #         else:
    #             combined_df.sort_index(inplace=True)
    #         if (entity.last_time_index is not None or
    #                 other[entity.id].last_time_index is not None):
    #             has_last_time_index.append(entity.id)

    #         combined_es.update_dataframe(
    #             entity_id=entity.id,
    #             df=combined_df,
    #             recalculate_last_time_indexes=False,
    #         )

    #     combined_es.add_last_time_indexes(updated_entities=has_last_time_index)
    #     self.reset_data_description()
    #     return combined_es

    # ###########################################################################
    # #  Indexing methods  ###############################################
    # ###########################################################################
    # def add_last_time_indexes(self, updated_entities=None):
    #     """
    #     Calculates the last time index values for each entity (the last time
    #     an instance or children of that instance were observed).  Used when
    #     calculating features using training windows
    #     Args:
    #         updated_entities (list[str]): List of entity ids to update last_time_index for
    #             (will update all parents of those entities as well)
    #     """
    #     # Generate graph of entities to find leaf entities
    #     children = defaultdict(list)  # parent --> child mapping
    #     child_vars = defaultdict(dict)
    #     for r in self.relationships:
    #         children[r.parent_dataframe.id].append(r.child_dataframe)
    #         child_vars[r.parent_dataframe.id][r.child_dataframe.id] = r.child_column

    #     updated_entities = updated_entities or []
    #     if updated_entities:
    #         # find parents of updated_entities
    #         parent_queue = updated_entities[:]
    #         parents = set()
    #         while len(parent_queue):
    #             e = parent_queue.pop(0)
    #             if e in parents:
    #                 continue
    #             parents.add(e)

    #             for parent_id, _ in self.get_forward_entities(e):
    #                 parent_queue.append(parent_id)

    #         queue = [self[p] for p in parents]
    #         to_explore = parents
    #     else:
    #         to_explore = set([e.id for e in self.dataframes[:]])
    #         queue = self.dataframes[:]

    #     explored = set()

    #     for e in queue:
    #         e.last_time_index = None

    #     # We will explore children of entities on the queue,
    #     # which may not be in the to_explore set. Therefore,
    #     # we check whether all elements of to_explore are in
    #     # explored, rather than just comparing length
    #     while not to_explore.issubset(explored):
    #         entity = queue.pop(0)

    #         if entity.last_time_index is None:
    #             if entity.time_index is not None:
    #                 lti = entity.df[entity.time_index].copy()
    #                 if isinstance(entity.df, dd.DataFrame):
    #                     # The current Dask implementation doesn't set the index of the dataframe
    #                     # to the entity's index, so we have to do it manually here
    #                     lti.index = entity.df[entity.index].copy()
    #             else:
    #                 lti = entity.df[entity.index].copy()
    #                 if isinstance(entity.df, dd.DataFrame):
    #                     lti.index = entity.df[entity.index].copy()
    #                     lti = lti.apply(lambda x: None)
    #                 elif is_instance(entity.df, ks, 'DataFrame'):
    #                     lti = ks.Series(pd.Series(index=lti.to_list(), name=lti.name))
    #                 else:
    #                     lti[:] = None
    #             entity.last_time_index = lti

    #         if entity.id in children:
    #             child_entities = children[entity.id]

    #             # if all children not explored, skip for now
    #             if not set([e.id for e in child_entities]).issubset(explored):
    #                 # Now there is a possibility that a child entity
    #                 # was not explicitly provided in updated_entities,
    #                 # and never made it onto the queue. If updated_entities
    #                 # is None then we just load all entities onto the queue
    #                 # so we didn't need this logic
    #                 for e in child_entities:
    #                     if e.id not in explored and e.id not in [q.id for q in queue]:
    #                         queue.append(e)
    #                 queue.append(entity)
    #                 continue

    #             # updated last time from all children
    #             for child_e in child_entities:
    #                 # TODO: Figure out if Dask code related to indexes is important for Koalas
    #                 if child_e.last_time_index is None:
    #                     continue
    #                 link_var = child_vars[entity.id][child_e.id].id

    #                 lti_is_dask = isinstance(child_e.last_time_index, dd.Series)
    #                 lti_is_koalas = is_instance(child_e.last_time_index, ks, 'Series')
    #                 if lti_is_dask or lti_is_koalas:
    #                     to_join = child_e.df[link_var]
    #                     if lti_is_dask:
    #                         to_join.index = child_e.df[child_e.index]

    #                     lti_df = child_e.last_time_index.to_frame(name='last_time').join(
    #                         to_join.to_frame(name=entity.index)
    #                     )

    #                     if lti_is_dask:
    #                         new_index = lti_df.index.copy()
    #                         new_index.name = None
    #                         lti_df.index = new_index
    #                     lti_df = lti_df.groupby(lti_df[entity.index]).agg('max')

    #                     lti_df = entity.last_time_index.to_frame(name='last_time_old').join(lti_df)

    #                 else:
    #                     lti_df = pd.DataFrame({'last_time': child_e.last_time_index,
    #                                            entity.index: child_e.df[link_var]})

    #                     # sort by time and keep only the most recent
    #                     lti_df.sort_values(['last_time', entity.index],
    #                                        kind="mergesort", inplace=True)

    #                     lti_df.drop_duplicates(entity.index,
    #                                            keep='last',
    #                                            inplace=True)

    #                     lti_df.set_index(entity.index, inplace=True)
    #                     lti_df = lti_df.reindex(entity.last_time_index.index)
    #                     lti_df['last_time_old'] = entity.last_time_index
    #                 if not (lti_is_dask or lti_is_koalas) and lti_df.empty:
    #                     # Pandas errors out if it tries to do fillna and then max on an empty dataframe
    #                     lti_df = pd.Series()
    #                 else:
    #                     if lti_is_koalas:
    #                         lti_df['last_time'] = ks.to_datetime(lti_df['last_time'])
    #                         lti_df['last_time_old'] = ks.to_datetime(lti_df['last_time_old'])
    #                         # TODO: Figure out a workaround for fillna and replace
    #                         lti_df = lti_df.max(axis=1)
    #                     else:
    #                         lti_df['last_time'] = lti_df['last_time'].astype('datetime64[ns]')
    #                         lti_df['last_time_old'] = lti_df['last_time_old'].astype('datetime64[ns]')
    #                         lti_df = lti_df.fillna(pd.to_datetime('1800-01-01 00:00')).max(axis=1)
    #                         lti_df = lti_df.replace(pd.to_datetime('1800-01-01 00:00'), pd.NaT)
    #                 # lti_df = lti_df.apply(lambda x: x.dropna().max(), axis=1)

    #                 entity.last_time_index = lti_df
    #                 entity.last_time_index.name = 'last_time'

    #         explored.add(entity.id)
    #     self.reset_data_description()

    # ###########################################################################
    # #  Other ###############################################
    # ###########################################################################

    # def add_interesting_values(self, max_values=5, verbose=False, entity_id=None):
    #     """Find interesting values for categorical variables, to be used to generate "where" clauses

    #     Args:
    #         max_values (int) : Maximum number of values per variable to add.
    #         verbose (bool) : If True, print summary of interesting values found.
    #         entity_id (str) : The Entity for which to add interesting values. If not specified
    #             interesting values will be added for all Entities.

    #     Returns:
    #         None

    #     """
    #     if entity_id:
    #         entities = [self[entity_id]]
    #     else:
    #         entities = self.dataframes
    #     for entity in entities:
    #         for variable in entity.variables:
    #             # some heuristics to find basic 'where'-able variables
    #             if isinstance(variable, vtypes.Discrete):
    #                 variable.interesting_values = pd.Series(dtype=variable.entity.df[variable.id].dtype)

    #                 # TODO - consider removing this constraints
    #                 # don't add interesting values for entities in relationships
    #                 skip = False
    #                 for r in self.relationships:
    #                     if variable in [r.child_column, r.parent_column]:
    #                         skip = True
    #                         break
    #                 if skip:
    #                     continue

    #                 counts = entity.df[variable.id].value_counts()

    #                 # find how many of each unique value there are; sort by count,
    #                 # and add interesting values to each variable
    #                 total_count = np.sum(counts)
    #                 counts[:] = counts.sort_values()[::-1]
    #                 for i in range(min(max_values, len(counts.index))):
    #                     idx = counts.index[i]

    #                     if len(counts.index) < 25:
    #                         if verbose:
    #                             msg = "Variable {}: Marking {} as an "
    #                             msg += "interesting value"
    #                             logger.info(msg.format(variable.id, idx))
    #                         variable.interesting_values = variable.interesting_values.append(pd.Series([idx]))
    #                     else:
    #                         fraction = counts[idx] / total_count
    #                         if fraction > 0.05 and fraction < 0.95:
    #                             if verbose:
    #                                 msg = "Variable {}: Marking {} as an "
    #                                 msg += "interesting value"
    #                                 logger.info(msg.format(variable.id, idx))
    #                             variable.interesting_values = variable.interesting_values.append(pd.Series([idx]))
    #                         else:
    #                             break

    #     self.reset_data_description()

    # def plot(self, to_file=None):
    #     """
    #     Create a UML diagram-ish graph of the EntitySet.

    #     Args:
    #         to_file (str, optional) : Path to where the plot should be saved.
    #             If set to None (as by default), the plot will not be saved.

    #     Returns:
    #         graphviz.Digraph : Graph object that can directly be displayed in
    #             Jupyter notebooks.

    #     """
    #     graphviz = check_graphviz()
    #     format_ = get_graphviz_format(graphviz=graphviz,
    #                                   to_file=to_file)

    #     # Initialize a new directed graph
    #     graph = graphviz.Digraph(self.id, format=format_,
    #                              graph_attr={'splines': 'ortho'})

    #     # Draw entities
    #     for entity in self.dataframes:
    #         variables_string = '\l'.join([var.id + ' : ' + var.type_string  # noqa: W605
    #                                       for var in entity.variables])
    #         if isinstance(entity.df, dd.DataFrame):  # entity is a dask entity
    #             label = '{%s |%s\l}' % (entity.id, variables_string)  # noqa: W605
    #         else:
    #             nrows = entity.shape[0]
    #             label = '{%s (%d row%s)|%s\l}' % (entity.id, nrows, 's' * (nrows > 1), variables_string)  # noqa: W605
    #         graph.node(entity.id, shape='record', label=label)

    #     # Draw relationships
    #     for rel in self.relationships:
    #         # Display the key only once if is the same for both related entities
    #         if rel._parent_column_id == rel._child_column_id:
    #             label = rel._parent_column_id
    #         else:
    #             label = '%s -> %s' % (rel._parent_column_id,
    #                                   rel._child_column_id)

    #         graph.edge(rel._child_dataframe_id, rel._parent_column_id, xlabel=label)

    #     if to_file:
    #         save_graph(graph, to_file, format_)
    #     return graph

    # def _handle_time(self, entity_id, df, time_last=None, training_window=None, include_cutoff_time=True):
    #     """
    #     Filter a dataframe for all instances before time_last.
    #     If the DataTable does not have a time index, return the original
    #     dataframe.
    #     """
    #     dt = self[entity_id]
    #     if is_instance(df, ks, 'DataFrame') and isinstance(time_last, np.datetime64):
    #         time_last = pd.to_datetime(time_last)
    #     if dt.time_index:
    #         df_empty = df.empty if isinstance(df, pd.DataFrame) else False
    #         if time_last is not None and not df_empty:
    #             if include_cutoff_time:
    #                 df = df[df[dt.time_index] <= time_last]
    #             else:
    #                 df = df[df[dt.time_index] < time_last]
    #             if training_window is not None:
    #                 training_window = _check_timedelta(training_window)
    #                 if include_cutoff_time:
    #                     mask = df[dt.time_index] > time_last - training_window
    #                 else:
    #                     mask = df[dt.time_index] >= time_last - training_window
    #                 if dt.last_time_index is not None:
    #                     lti_slice = dt.last_time_index.reindex(df.index)
    #                     if include_cutoff_time:
    #                         lti_mask = lti_slice > time_last - training_window
    #                     else:
    #                         lti_mask = lti_slice >= time_last - training_window
    #                     mask = mask | lti_mask
    #                 else:
    #                     warnings.warn(
    #                         "Using training_window but last_time_index is "
    #                         "not set on entity %s" % (dt.id)
    #                     )

    #                 df = df[mask]

    #     for secondary_time_index, columns in dt.secondary_time_index.items():
    #         # should we use ignore time last here?
    #         df_empty = df.empty if isinstance(df, pd.DataFrame) else False
    #         if time_last is not None and not df_empty:
    #             mask = df[secondary_time_index] >= time_last
    #             if isinstance(df, dd.DataFrame):
    #                 for col in columns:
    #                     df[col] = df[col].mask(mask, np.nan)
    #             elif is_instance(df, ks, 'DataFrame'):
    #                 df.loc[mask, columns] = None
    #             else:
    #                 df.loc[mask, columns] = np.nan

    #     return df

    # def query_by_values(self, entity_id, instance_vals, variable_id=None, columns=None,
    #                     time_last=None, training_window=None, include_cutoff_time=True):
    #     """Query instances that have variable with given value

    #     Args:
    #         entity_id (str): The id of the entity to query
    #         instance_vals (pd.Dataframe, pd.Series, list[str] or str) :
    #             Instance(s) to match.
    #         variable_id (str) : Variable to query on. If None, query on index.
    #         columns (list[str]) : Columns to return. Return all columns if None.
    #         time_last (pd.TimeStamp) : Query data up to and including this
    #             time. Only applies if entity has a time index.
    #         training_window (Timedelta, optional):
    #             Window defining how much time before the cutoff time data
    #             can be used when calculating features. If None, all data before cutoff time is used.
    #         include_cutoff_time (bool):
    #             If True, data at cutoff time are included in calculating features

    #     Returns:
    #         pd.DataFrame : instances that match constraints with ids in order of underlying dataframe
    #     """
    #     entity = self[entity_id]
    #     if not variable_id:
    #         variable_id = entity.index

    #     instance_vals = _vals_to_series(instance_vals, variable_id)

    #     training_window = _check_timedelta(training_window)

    #     if training_window is not None:
    #         assert training_window.has_no_observations(), "Training window cannot be in observations"

    #     if instance_vals is None:
    #         df = entity.df.copy()

    #     elif isinstance(instance_vals, pd.Series) and instance_vals.empty:
    #         df = entity.df.head(0)

    #     else:
    #         if is_instance(instance_vals, (dd, ks), 'Series'):
    #             df = entity.df.merge(instance_vals.to_frame(), how="inner", on=variable_id)
    #         elif isinstance(instance_vals, pd.Series) and is_instance(entity.df, ks, 'DataFrame'):
    #             df = entity.df.merge(ks.DataFrame({variable_id: instance_vals}), how="inner", on=variable_id)
    #         else:
    #             df = entity.df[entity.df[variable_id].isin(instance_vals)]

    #         if isinstance(entity.df, pd.DataFrame):
    #             df = df.set_index(entity.index, drop=False)

    #         # ensure filtered df has same categories as original
    #         # workaround for issue below
    #         # github.com/pandas-dev/pandas/issues/22501#issuecomment-415982538
    #         if pdtypes.is_categorical_dtype(entity.df[variable_id]):
    #             categories = pd.api.types.CategoricalDtype(categories=entity.df[variable_id].cat.categories)
    #             df[variable_id] = df[variable_id].astype(categories)

    #     df = self._handle_time(entity_id=entity_id,
    #                            df=df,
    #                            time_last=time_last,
    #                            training_window=training_window,
    #                            include_cutoff_time=include_cutoff_time)

    #     if columns is not None:
    #         df = df[columns]

    #     return df

    # def update_dataframe(self, entity_id, df, already_sorted=False, recalculate_last_time_indexes=True):
    #     # --> necessary for woodwork
    #     '''Update entity's internal dataframe, optionaly making sure data is sorted,
    #     reference indexes to other entities are consistent, and last_time_indexes
    #     are consistent.
    #     '''
    #     variables = self[entity_id].variables
    #     if len(df.columns) != len(variables):
    #         raise ValueError("Updated dataframe contains {} columns, expecting {}".format(len(df.columns),
    #                                                                                       len(variables)))
    #     for v in variables:
    #         if v.id not in df.columns:
    #             raise ValueError("Updated dataframe is missing new {} column".format(v.id))

    #     # Make sure column ordering matches variable ordering
    #     self[entity_id].df = df[[v.id for v in variables]]
    #     self[entity_id].set_index(self[entity_id].index)
    #     if self[entity_id].time_index is not None:
    #         self[entity_id].set_time_index(self[entity_id].time_index, already_sorted=already_sorted)
    #         self._check_uniform_time_index(self[entity_id])
    #     self.set_secondary_time_index(self[entity_id], self[entity_id].secondary_time_index)
    #     if recalculate_last_time_indexes and self[entity_id].last_time_index is not None:
    #         self.add_last_time_indexes(updated_entities=[self[entity_id].id])
    #     self.reset_data_description()
    def _check_time_indexes(self):
        for dataframe in self.dataframe_dict.values():
            self._check_uniform_time_index(dataframe)
            self._check_secondary_time_index(dataframe)

    def _check_secondary_time_index(self, dataframe, secondary_time_index=None):
        secondary_time_index = secondary_time_index or dataframe.ww.metadata.get('secondary_time_index', {})
        for time_index, columns in secondary_time_index.items():
            self._check_uniform_time_index(dataframe, column_id=time_index)
            if time_index not in columns:
                columns.append(time_index)

    def _check_uniform_time_index(self, dataframe, column_id=None):
        column_id = column_id or dataframe.ww.time_index
        if column_id is None:
            return

        time_type = self._get_time_type(dataframe, column_id)
        if self.time_type is None:
            self.time_type = time_type
        elif self.time_type != time_type:
            info = "%s time index is %s type which differs from other entityset time indexes"
            raise TypeError(info % (dataframe.ww.name, time_type))

    def _get_time_type(self, dataframe, column_id=None):
        variable_id = column_id or dataframe.ww.time_index
        if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
            dtype = dataframe.ww.physical_types[column_id]
            time_to_check = vtypes.DEFAULT_DTYPE_VALUES[dtype]
        else:
            time_to_check = dataframe[column_id].iloc[0]

        time_type = _check_time_type(time_to_check)
        if time_type is None:
            info = "%s time index not recognized as numeric or datetime"
            raise TypeError(info % dataframe.ww.name)
        return time_type


def _vals_to_series(instance_vals, variable_id):
    """
    instance_vals may be a pd.Dataframe, a pd.Series, a list, a single
    value, or None. This function always returns a Series or None.
    """
    if instance_vals is None:
        return None

    # If this is a single value, make it a list
    if not hasattr(instance_vals, '__iter__'):
        instance_vals = [instance_vals]

    # convert iterable to pd.Series
    if isinstance(instance_vals, pd.DataFrame):
        out_vals = instance_vals[variable_id]
    elif is_instance(instance_vals, (pd, dd, ks), 'Series'):
        out_vals = instance_vals.rename(variable_id)
    else:
        out_vals = pd.Series(instance_vals)

    # no duplicates or NaN values
    out_vals = out_vals.drop_duplicates().dropna()

    # want index to have no name for the merge in query_by_values
    out_vals.index.name = None

    return out_vals