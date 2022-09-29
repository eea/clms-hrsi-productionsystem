from enum import Enum


class MajaMode(Enum):
    '''Maja execution mode.'''
    
    # Lower-case, as in the database
    nominal = 1
    backward = 2
    init = 3

    def to_string(mode) -> str:
        maja_mode_strings = {
            MajaMode.nominal: 'nominal',
            MajaMode.backward: 'backward',
            MajaMode.init: 'init'
        }
        return maja_mode_strings[mode]
