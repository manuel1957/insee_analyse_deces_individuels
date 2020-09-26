# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0112
# pylint: disable=W0703, R1721
"""
Created on Sat Sep 26 14:38:13 2020.

@author: manuel

@subjetc: outils communs
"""
import datetime


def tracage(message_l, fichiers_traces_l):
    """
    Ecrit les messages à la fois sur le fichier de sortie et dans la console.

    Parameters
    ----------

    message_l:  texte
        message à écrire sur le terminal et dans le fichier de traces.

    fichiers_traces_l: handler
        pointeur sur le fichier de traces.

    Returns
    -------
    None.

    """
    message_s = "%s:  %s " % (datetime.datetime.now(), message_l)
    print(message_s)
    fichiers_traces_l.write("%s\n" % message_s)
    fichiers_traces_l.flush()
