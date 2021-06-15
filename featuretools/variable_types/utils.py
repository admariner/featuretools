from warnings import warn

from woodwork import list_logical_types


def list_variable_types():
    """
    Retrieves all Variable types as a dataframe, with the column headers
        of name, type_string, and description.

    Args:
        None

    Returns:
        variable_types (pd.DataFrame): a DataFrame with column headers of
            name, type_strings, and description.
    """
    message = 'list_variable_types has been deprecated. Please use featuretools.list_logical_types instead.'
    warn(message=message, category=FutureWarning)
    return list_logical_types()
