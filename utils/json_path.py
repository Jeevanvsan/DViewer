from typing import List, Dict, Iterator, Any, Union


    


def get_json_keypaths(
                        input: Union[List[Dict[str, Any]], Dict[str, Any]],
                        segment_type: Union[str,List[str]],
                        parent: str = None, last_yield_path: str = None,
                        top_level: bool = False, combine_values: bool =
                        False, debug: bool = False) -> (
        Iterator)[Union[str, Dict[str, Any], List[Dict[str, Any]]]]:
    """
    Input - Dict
    Output - List
    This function get return items that match with the segment_type.
    segment_type:- This where provided on function call. By using this we
                    find the match case.
    parent:- Previous path
    cur_path:- Current path
    top_level:- If the value is True then check from the top level path is
                matching, otherwise check current level ends with it. If
                there is any match with this then the 'found' will be set
                as True
    combine_values:- It's a True or False. If it's True then return joined
                        value of the given v. If It's False then it will
                        return v.
    """

    ignore_keys = ["newline", "whitespace", "dot", "comma", "bracketed",
                    "start_bracket", "end_bracket", "bracketed_arguments"]
    if isinstance(segment_type, str):
        segment_type = [segment_type]
    segment_type_str = ".".join(segment_type)

    if isinstance(input, list):
        for v in input:
            yield from get_json_keypaths(
                v, segment_type, parent, last_yield_path, top_level,
                combine_values, debug)
    elif isinstance(input, dict):
        for k, v in input.items():
            if k not in ignore_keys:
                cur_path = (parent + "." + str(k) if parent is not None
                            else str(k))
            else:
                cur_path = parent
            # print("cur_path: ", cur_path, )
            found = False
            if debug:
                print("current path: ", cur_path)
            if cur_path is not None:
                if top_level:
                    if cur_path == segment_type_str:
                        found = True
                else:
                    if cur_path.endswith(segment_type_str):
                        found = True
                if found and (last_yield_path is None or last_yield_path
                                != cur_path):
                    last_yield_path = cur_path
                    if combine_values:
                        yield "".join(combineValues(v))
                    else:
                        yield v
            if isinstance(v, dict):
                yield from get_json_keypaths(
                    v, segment_type, cur_path, last_yield_path, top_level,
                    combine_values, debug)
            elif isinstance(v, list):
                for s in v:
                    yield from get_json_keypaths(
                        s, segment_type, cur_path, last_yield_path,
                        top_level, combine_values, debug)
            else:
                found = False
                if debug:
                    print("current path: ", parent)
                if parent is not None:
                    if top_level:
                        if parent == segment_type_str:
                            found = True
                    else:
                        if parent.endswith(segment_type_str):
                            found = True
                    if found and (last_yield_path is None or
                                    last_yield_path != parent):
                        if combine_values:
                            yield "".join(combineValues(input))
                        else:
                            yield input
def rtn_get_json_keypaths(
            _input: Union[List[Dict[str, Any]], Dict[str, Any]],
            segment_type: Union[str, List[str]],
            parent: str = None, last_yield_path: str = None,
            top_level: bool = False, combine_values: bool = False,
            debug: bool = False):
        out = list(get_json_keypaths(_input, segment_type, parent,
                                        last_yield_path, top_level,
                                        combine_values, debug))

        return out

def combineValues(val: Union[Dict,str, List[str]]) -> Iterator[str]:
    """
    Input:- dict, list, str
    Output:- item generator
    If the input where
    1)  {key_1: val_1, key_2: val_2}    ==>     [val_1, val_2]
    2)  {key_1: val_1, key_2: [val_2_1, val_2_2]}   ==>
                                            ['val_1', 'val_2_1', 'val_2_2']
    3)  [val_1, {key_2: val_2}]     ==>     [val_1, val_2]
    4)  [val_1, val_2]      ==>     [val_1, val_2]
    5)  val_1       ==>     [val_1]
    """
    if val is None:
        return
    if isinstance(val,str):
        yield val
    elif isinstance(val, list):
        for v in val:
            if isinstance(v, dict):
                yield from combineValues(v)
            elif isinstance(v,list):
                for s in v:
                    yield from combineValues(s)
            else:
                yield v
    else:
        # print("val:", val)
        for k,v in val.items():
            if isinstance(v, dict):
                yield from combineValues(v)
            elif isinstance(v,list):
                for s in v:
                    yield from combineValues(s)
            else:
                yield v