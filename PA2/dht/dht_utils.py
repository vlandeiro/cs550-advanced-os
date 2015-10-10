def check_type(var_name, var_type, true_type):
    if var_type != true_type:
        raise TypeError("%s type is %s but it should be %s." % (var_name, var_type, true_type))
