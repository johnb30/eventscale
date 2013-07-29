from __future__ import division
import pandas as pd
import os


def _get_data(path):
    """Private function to get the absolute path to the installed files."""
    cwd = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(cwd, 'data', path)


class Scale():
    def __init__(self, data, merge_var=None):
        self.data = data
        self.orig_vars = data.columns.tolist()
        if not merge_var:
            print 'Must provide a variable on which to merge!'
        else:
            self.merge_var = merge_var
        if self.merge_var not in self.orig_vars:
            print 'The `merge_var` variable does not exist in the oringal \
                    data!'

        self.columns = [self.merge_var, 'total_count']
        self.dataset_map = {'daily': 'daily_scale.csv',
                            'monthly': 'monthly_scale.csv',
                            'yearly': 'yearly_scale.csv',
                            'daily_monadic': 'daily_scale_monadic.csv',
                            'monthly_monadic': 'monthly_scale_monadic.csv',
                            'yearly_monadic': 'yearly_scale_monadic.csv'
                            }

    def _prep_data(self, in_data, col_names=None):
        data = pd.read_csv(_get_data(in_data), sep='\t', names=self.col_names)
        data[self.merge_var] = data[self.merge_var].map(lambda x: int(x))

        return data

    def _transform_data(self, dataset, new_name=None, old_name=None,):
        try:
            hold_data = pd.merge(self.data, dataset, on=self.merge_name)
        except KeyError:
            print 'The variable names for merging do not match.'

            hold_data[new_name] = (hold_data[old_name] /
                                   hold_data['total_count'])

        output_data = hold_data[self.orig_names + [new_name]]

        return output_data

    def scale(self, scale=None, new_var='scaled_count', old_var=None, outpath=None):
        if not scale:
            print 'Please indicate which type of scaling should be used!'

        scale_data = self._prep_data(self.dataset_map[scale])

        if not old_var:
            print 'Please indicate which variable should be scaled.'
        else:
            final_data = self._transform_data(scale_data, old_name=old_var,
                                              new_name=new_var)

        if outpath:
            final_data.to_csv(outpath, sep='\t', index=False)
        else:
            return final_data
