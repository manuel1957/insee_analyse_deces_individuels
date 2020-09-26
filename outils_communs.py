# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0112
# pylint: disable=W0703, R1721
"""
Created on Sat Sep 26 14:38:13 2020.

@author: manuel

@subjetc: outils communs
"""
import datetime
from mysql.connector import connect
from parametrage_application import parametres_connexion

departements = ["", "FR", "01", "02", "03", "04", "05", "06", "07", "08",
                "09", "10", "11", "12", "13", "14", "15", "16", "17", "18",
                "19", "21", "22", "23", "24", "25", "26", "27", "28", "29",
                "2A", "2B", "30", "31", "32", "33", "34", "35", "36", "37",
                "38", "39", "40", "41", "42", "43", "44", "45", "46", "47",
                "48", "49", "50", "51", "52", "53", "54", "55", "56", "57",
                "58", "59", "60", "61", "62", "63", "64", "65", "66", "67",
                "68", "69", "70", "71", "72", "73", "74", "75", "76", "77",
                "78", "79", "80", "81", "82", "83", "84", "85", "86", "87",
                "88", "89", "90", "91", "92", "93", "94", "95", "97"]

liste_departements = {"01": "l'Ain", "02": "l'Aisne", "03": "l'Allier",
                      "04": "les Alpes-de-Haute-Provence",
                      "05": "les Hautes-Alpes",
                      "06": "les Alpes-Maritimes", "07": "l'Ardèche",
                      "08": "les Ardennes", "09": "l'Ariège",
                      "10": "l'Aube", "11": "l'Aude", "12": "l'Aveyron",
                      "13": "les Bouches-du-Rhône", "14": "le Calvados",
                      "15": "le Cantal", "16": "la Charente",
                      "17": "la Charente-Maritime", "18": "le Cher",
                      "19": "la Corrèze", "21": "la Côte-d'Or",
                      "22": "les Côtes-d'Armor", "23": "la Creuse",
                      "24": "la Dordogne", "25": "le Doubs", "26": "la Drôme",
                      "27": "l'Eure", "28": "l'Eure-et-Loir",
                      "29": "le Finistère", "2A": "la Corse-du-Sud",
                      "2B": "la Haute-Corse", "30": "le Gard",
                      "31": "la Haute-Garonne", "32": "le Gers",
                      "33": "la Gironde", "34": "l'Hérault",
                      "35": "l'Ille-et-Vilaine", "36": "l'Indre",
                      "37": "l'Indre-et-Loire", "38": "l'Isère",
                      "39": "le Jura", "40": "les Landes",
                      "41": "le Loir-et-Cher", "42": "la Loire",
                      "43": "la Haute-Loire", "44": "la Loire-Atlantique",
                      "45": "le Loiret", "46": "le Lot",
                      "47": "le Lot-et-Garonne", "48": "la Lozère",
                      "49": "le Maine-et-Loire", "50": "la Manche",
                      "51": "la Marne", "52": "la Haute-Marne",
                      "53": "la Mayenne", "54": "la Meurthe-et-Moselle",
                      "55": "la Meuse", "56": "le Morbihan",
                      "57": "la Moselle", "58": "la Nièvre", "59": "le Nord",
                      "60": "l'Oise", "61": "l'Orne", "62": "le Pas-de-Calais",
                      "63": "le Puy-de-Dôme", "64": "les Pyrénées-Atlantiques",
                      "65": "les Hautes-Pyrénées",
                      "66": "les Pyrénées-Orientales", "67": "le Bas-Rhin",
                      "68": "le Haut-Rhin", "69": "le Rhône",
                      "70": "la Haute-Saône", "71": "la Saône-et-Loire",
                      "72": "la Sarthe", "73": "la Savoie",
                      "74": "la Haute-Savoie", "75": "Paris",
                      "76": "la Seine-Maritime", "77": "la Seine-et-Marne",
                      "78": "les Yvelines", "79": "les Deux-Sèvres",
                      "80": "la Somme", "81": "le Tarn",
                      "82": "le Tarn-et-Garonne", "83": "le Var",
                      "84": "le Vaucluse", "85": "la Vendée",
                      "86": "la Vienne", "87": "la Haute-Vienne",
                      "88": "les Vosges", "89": "l'Yonne",
                      "90": "le Territoire de Belfort", "91": "l'Essonne",
                      "92": "les Hauts-de-Seine", "93": "la Seine-Saint-Denis",
                      "94": "le Val-de-Marne", "95": "le Val-d'Oise",
                      "97":  "l'outremer", "FR": "la France et l'outremer"}


def tracage(message_l, fichiers_traces_l):
    """
    Ecrit les messages à la fois sur le fichier de sortie et dans la console.

    Parameters.
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


def connexion_mysql():
    """
    Se connecte à la base et renvoie un cuseur.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    mabase = connect(
        host=parametres_connexion["host"],
        user=parametres_connexion["user"],
        password=parametres_connexion["password"],
        database=parametres_connexion["database"]
    )
    # demande du curseur
    return mabase, mabase.cursor()
