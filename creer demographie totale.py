# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0112
# pylint: disable=W0703
"""
Created on Wed Sep  2 09:49:15 2020.

@author: manuel

Reconstitution des données démographiques par département.
"""

import os
import traceback
import codecs
import datetime
import re
import xlrd
import numpy as np


def tracage(message_l):
    """
    Ecrit les messages à la fois sur le fichier de sortie et dans la console.

    Parameters
    ----------
    message_l : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    message_l = "%s : %s\n" % (datetime.datetime.now(), message_l)
    print(message_l)
    fichier_traces.write(message_l)
    fichier_traces.flush()


def accroissement_pop(departement_l, table_acompleter, an_deb_l, an_fin_l,
                      taux, derniere=False):
    """
    Met à jour la table de démographie sur la periode an_deb_l +1 an_fin_l -1.

    avec le taux d'accoissement donné.
    Si on est à la dernière période on va jusqu'à an_fin_l.

    Parameters
    ----------
    departement_l : char
        DESCRIPTION.
    table_acompleter : dict
        DESCRIPTION.
    an_deb_l : int
        DESCRIPTION.
    an_fin_l : int
        DESCRIPTION.
    taux : int
        DESCRIPTION.
    derniere : bool, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    table_acompleter : dict
        DESCRIPTION.

    """

    if derniere:
        an_fin_l = an_fin_l + 1
    pop_encours = pop_debut
    taux_mensuel = int(taux / 12)
    if departement_l not in table_acompleter:
        table_acompleter[departement_l] = {}

    for an_l in range(an_deb_l, an_fin_l + 1):
        if an_l not in table_acompleter[departement_l]:
            table_acompleter[departement_l][an_l] = {}
        for mois_l in range(1, 13):
            table_acompleter[departement_l][an_l][mois_l] = 0
            table_acompleter[departement_l][an_l][mois_l] += \
                pop_encours + taux_mensuel
            pop_encours += taux_mensuel
    return table_acompleter
#
# Programme principal
#
#


repertoire_donnees = r'D:\donnees_manu\Documents\insee\divers'
repertoire_cible = os.path.join(repertoire_donnees, "resultats")
fichier_traces = codecs.open('cr extraction démographie.txt', 'w', 'UTF-8')
fichier_donnees = os.path.join(repertoire_donnees,
                               'Fichier_poplegale_6817.xls')
table_demographie = {}
table_demographie['FR'] = {}

debut = datetime.datetime.now()
date_jour = debut.strftime("%Y_%m_%d")
tracage(u"Début d'extraction des données démographiques")
try:

    fichier_xls = xlrd.open_workbook(fichier_donnees)
    tracage("fin lecture fichier excel")
    liste_nom_feuilles = fichier_xls.sheet_names()
    for i in np.arange(len(liste_nom_feuilles)):
        print(liste_nom_feuilles[i])
        if re.search("[0-9]*", liste_nom_feuilles[i]).group() == '':
            continue
        annee = int(liste_nom_feuilles[i])

        feuille = fichier_xls.sheet_by_name(liste_nom_feuilles[i])
        num_rows = feuille.nrows
        num_cells = feuille.ncols
        # print('num_rows, num_cells', num_rows, num_cells)

        donnees = False
        curr_row = 0
        while curr_row < num_rows:
            row = feuille.row(curr_row)
            # print row, len(row), row[0], row[1]
            # print('Row: ', curr_row)
            # print(row, len(row))
            if row[1].value.find('NCC') != -1:
                donnees = True
                curr_row += 1
                row = feuille.row(curr_row)
            # curr_cell = 0
            # while curr_cell < num_cells:
            #     cell_type = feuille.cell_type(curr_row, curr_cell)
            #     cell_value = feuille.cell_value(curr_row, curr_cell)
            #     print(' ', cell_type, ':', cell_value)
            #     curr_cell += 1
            if donnees:
                departement = str(row[0].value)[0:2]
                pop = int(row[2].value)
                if departement not in table_demographie:
                    table_demographie[departement] = {}
                if annee not in table_demographie[departement]:
                    table_demographie[departement][annee] = 0
                table_demographie[departement][annee] += int(pop)
                if annee not in table_demographie['FR']:
                    table_demographie['FR'][annee] = 0
                table_demographie['FR'][annee] += int(pop)

            curr_row += 1
    table_demographie_complete = {}
    for departement in table_demographie:
        if departement.find("20") != -1:
            continue
        tracage("Traitement du département : %s" % departement)
        annees = [x for x in table_demographie[departement].keys()]
        annees.sort()
        j = 0
        annee_en_cours = datetime.datetime.now().year
        annee_debut = annees[j]
        annee_fin = annees[j+1]

        while annee_fin <= annee_en_cours:
            pop_debut = table_demographie[departement][annee_debut]
            diff_an = annee_fin - annee_debut
            diff_pop = table_demographie[departement][annee_fin] - pop_debut
            accr_ann = int(diff_pop / diff_an)
            table_demographie_complete = \
                accroissement_pop(departement, table_demographie_complete,
                                  annee_debut, annee_fin, accr_ann)

            j += 1
            if j + 1 >= len(annees):
                # on conserve le taux d'acroissemnt précédent
                table_demographie_complete = \
                    accroissement_pop(departement, table_demographie_complete,
                                      annees[j], annee_en_cours, accr_ann,
                                      derniere=True)
                annee_fin = annee_en_cours + 1
            else:
                annee_debut = annees[j]
                annee_fin = annees[j+1]
        # creer fichier deépartement
        fichier_resultat = open(os.path.join(repertoire_cible,
                                             "demographie_dep_%s.csv"
                                             % departement),
                                'w')
        annees = [x for x in table_demographie_complete[departement].keys()]
        annees.sort()
        for an in annees:
            for mois in range(1, 13):
                fichier_resultat.write("%s\t%s\t%s\n"
                                       % (an, mois,
                                          table_demographie_complete[departement][an][mois]))

        fichier_resultat.close()

except BaseException as erreur:
    # print('feuille %s row %s' % (feuille, row))
    tracage(traceback.print_exc())
    tracage("erreur : %s" % erreur)

finally:
    tracage("Temps d'exécution : %s " % (datetime.datetime.now() - debut))
    fichier_traces.close()
