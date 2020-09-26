# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0112
# pylint: disable=W0703, R1721
"""
Created on Sat Aug  1 12:37:22 2020.

@author: manuel

Objet: extraction des données et génération images.
"""


import os
import codecs
import datetime
import traceback
from mysql.connector import connect
import mysql.connector
import matplotlib.pyplot as plt
import numpy as np


def tracage(message_l):
    """
    Ecrit les messages à la fois sur le fichier de sortie et dans la console.

    Parameters
    ----------

    message_l:  texte
        DESCRIPTION.

    Returns
    -------
    None.

    """
    message_s = "%s:  %s " % (datetime.datetime.now(), message_l)
    print(message_s)
    fichier_traces.write("%s\n" % message_s)
    fichier_traces.flush()


# programme principal

fichier_traces = codecs.open(os.path.join('logs', 'recherche_deces_dep.txt'),
                             'a', 'UTF-8')
repertoire_demographie = r'D:\donnees_manu\Documents\insee\divers\resultats'
debut = datetime.datetime.now()
tracage(u"Début de recherche")

nb_maj = 0

try:

    # Connexion au serveur Mysql
    mydb = connect(
        host="localhost",
        user="manuel",
        password="manu",
        database="deces_insee"
    )
    # demande du curseur
    curseur = mydb.cursor()
    # commande = """select distinct(lieudeces) from deces;"""
    # curseur.execute(commande)
    # tracage('fin du select départements')
    # departements = curseur.fetchall()
    # departements = [str(x[0])[0:2] for x in departements]
    # departements = set(departements)
    departements = ["", "FR", "01", "02", "03", "04", "05", "06",
                    "07", "08", "09", "10", "11", "12", "13", "14",
                    "15", "16", "17", "18", "19", "21", "22",
                    "23", "24", "25", "26", "27", "28", "29", "2A",
                    "2B", "30", "31", "32", "33", "34", "35", "36",
                    "37", "38", "39", "40", "41", "42", "43", "44",
                    "45", "46", "47", "48", "49", "50", "51", "52",
                    "53", "54", "55", "56", "57", "58", "59", "60",
                    "61", "62", "63", "64", "65", "66", "67", "68",
                    "69", "70", "71", "72", "73", "74", "75", "76",
                    "77", "78", "79", "80", "81", "82", "83", "84",
                    "85", "86", "87", "88", "89", "90", "91", "92",
                    "93", "94", "95", "97"]
    liste_departements = {"01": "l'Ain", "02": "l'Aisne", "03": "l'Allier",
                          "04": "les Alpes-de-Haute-Provence",
                          "05": "les Hautes-Alpes",
                          "06": "les Alpes-Maritimes", "07": "l'Ardèche",
                          "08": "les Ardennes", "09": "l'Ariège",
                          "10": "l'Aube",
                          "11": "l'Aude", "12": "l'Aveyron",
                          "13": "les Bouches-du-Rhône",
                          "14": "le Calvados", "15": "le Cantal",
                          "16": "la Charente",
                          "17": "la Charente-Maritime", "18": "le Cher",
                          "19": "la Corrèze", "21": "la Côte-d'Or",
                          "22": "les Côtes-d'Armor", "23": "la Creuse",
                          "24": "la Dordogne", "25": "le Doubs",
                          "26": "la Drôme",
                          "27": "l'Eure", "28": "l'Eure-et-Loir",
                          "29": "le Finistère",
                          "2A": "la Corse-du-Sud",
                          "2B": "la Haute-Corse", "30": "le Gard",
                          "31": "la Haute-Garonne", "32": "le Gers",
                          "33": "la Gironde", "34": "l'Hérault",
                          "35": "l'Ille-et-Vilaine", "36": "l'Indre",
                          "37": "l'Indre-et-Loire", "38": "l'Isère",
                          "39": "le Jura",
                          "40": "les Landes", "41": "le Loir-et-Cher",
                          "42": "la Loire",
                          "43": "la Haute-Loire", "44": "la Loire-Atlantique",
                          "45": "le Loiret", "46": "le Lot",
                          "47": "le Lot-et-Garonne",
                          "48": "la Lozère", "49": "le Maine-et-Loire",
                          "50": "la Manche", "51": "la Marne",
                          "52": "la Haute-Marne",
                          "53": "la Mayenne", "54": "la Meurthe-et-Moselle",
                          "55": "la Meuse", "56": "le Morbihan",
                          "57": "la Moselle",
                          "58": "la Nièvre", "59": "le Nord", "60": "l'Oise",
                          "61": "l'Orne", "62": "le Pas-de-Calais",
                          "63": "le Puy-de-Dôme",
                          "64": "les Pyrénées-Atlantiques",
                          "65": "les Hautes-Pyrénées",
                          "66": "les Pyrénées-Orientales", "67": "le Bas-Rhin",
                          "68": "le Haut-Rhin", "69": "le Rhône",
                          "70": "la Haute-Saône", "71": "la Saône-et-Loire",
                          "72": "la Sarthe", "73": "la Savoie",
                          "74": "la Haute-Savoie",
                          "75": "Paris", "76": "la Seine-Maritime",
                          "77": "la Seine-et-Marne", "78": "les Yvelines",
                          "79": "les Deux-Sèvres", "80": "la Somme",
                          "81": "le Tarn",
                          "82": "le Tarn-et-Garonne", "83": "le Var",
                          "84": "le Vaucluse", "85": "la Vendée",
                          "86": "la Vienne",
                          "87": "la Haute-Vienne", "88": "les Vosges",
                          "89": "l'Yonne",
                          "90": "le Territoire de Belfort", "91": "l'Essonne",
                          "92": "les Hauts-de-Seine",
                          "93": "la Seine-Saint-Denis",
                          "94": "le Val-de-Marne", "95": "le Val-d'Oise",
                          "97":  "l'outremer", "FR": "la France et l'outremer"}

    repertoire_graphiques = "Graphiques_%s" % \
        datetime.datetime.now().strftime("%Y_%m_%d")
    if not os.path.exists(repertoire_graphiques):
        os.makedirs(repertoire_graphiques)

    for dep in departements:
        if len(dep) == 0:
            continue
        if dep.find("FR") != -1:
            commande = """
            SELECT  year(date_deces) as txn_annee, month(date_deces)
            AS txn_mois,
            count(*) as deces_mensuels FROM deces
            GROUP BY   txn_annee, txn_mois
            order by  txn_annee, txn_mois"""
        else:
            commande = """
            SELECT  year(date_deces) as txn_annee, month(date_deces)
            AS txn_mois,
            count(*) as deces_mensuels FROM deces  where `lieudeces`
            like '%s%%' GROUP BY   txn_annee, txn_mois
            order by  txn_annee, txn_mois""" % dep
        curseur.execute(commande)

        tracage('fin du select dep %s ' % dep)
        liste_dep = curseur.fetchall()
        f_out_t = os.path.join(repertoire_graphiques,
                               "deces_demog_%s.csv" % dep)
        f_out = codecs.open(f_out_t, 'w', 'UTF-8')
        f_out.write(u"Année \tMois \tNb décès \tNb habitants"
                    u" \tTaux mortalité\n")

        # lecture du fichier démographie du département
        table_demo = {}
        fichier_demog = open(os.path.join(repertoire_demographie,
                                          "demographie_dep_%s.csv" % dep))
        lignes = fichier_demog.readlines()
        fichier_demog.close()

        for ligne in lignes:
            annee, mois, pop = ligne.split('\t')
            annee = int(annee)
            mois = int(mois)
            if annee not in table_demo:
                table_demo[annee] = {}
            if mois not in table_demo[annee]:
                table_demo[annee][mois] = 0
            table_demo[annee][mois] = int(pop.strip('\n'))

        fig = plt.figure(figsize=(16, 16), dpi=72)

        # ax = fig.add_subplot(111)
        donnees = {}
        max_dc = {}
        min_dc = {}
        moy_dc = {}
        maximum = 0
        date_max = ""
        for i in range(1, 13):
            max_dc[i] = 0
            min_dc[i] = 9e+6
            moy_dc[i] = 0

        nb_annees = 0
        for annee, mois, deces in liste_dep:
            # pour l'avoir en pourcentage on multiplie le nombre de décès
            # par 100
            deces = deces * 100

            if annee < 1989 or annee not in table_demo or\
                    mois not in table_demo[annee]:
                continue
            if annee not in donnees:
                donnees[annee] = {}
                nb_annees += 1
            if mois not in donnees[annee]:
                donnees[annee][mois] = 0

            donnee_redressee = deces / table_demo[annee][mois]
            donnee_redressee_str = "%.3f" % donnee_redressee
            f_out.write("%s\t%s\t%s\t%s\t%s\n" %
                        (annee, mois, deces, table_demo[annee][mois],
                         donnee_redressee_str.replace('.', ',')))
            donnees[annee][mois] = donnee_redressee
            if annee < 2020:
                if donnee_redressee > max_dc[mois]:
                    max_dc[mois] = donnee_redressee
                    if donnee_redressee > maximum:
                        maximum = donnee_redressee
                        date_max = "%s/%s : %.2f %% " % (mois, annee, maximum)
                if donnee_redressee < min_dc[mois]:
                    min_dc[mois] = donnee_redressee
                moy_dc[mois] += donnee_redressee
        for i in range(1, 13):
            if nb_annees != 0:
                moy_dc[i] = moy_dc[i] / nb_annees
        f_out.close()
        mois = {}
        for i in range(1, 13):
            if i not in mois:
                mois[i] = []
            for j in donnees:
                if i in donnees[j]:
                    mois[i].append(donnees[j][i])
        std_l = {}
        for i in range(1, 13):
            if i not in std_l:
                std_l[i] = 0
            val = np.array([x for x in mois[i]])
            std_l[i] = np.std(val)

        # print(std_l)

        for annee in donnees:
            # marker_l = "v"
            # ls_l = '--'
            # if annee == 2020:
            #     marker_l = "o"
            #     ls_l = ':'
            mois = np.array([x for x in donnees[annee].keys()])
            valeurs = np.array([x for x in donnees[annee].values()])

            if annee == 2020:
                marker_l = "o"
                ls_l = ':'
                plt.plot(mois, valeurs, label='%s' % annee, ls=ls_l,
                         marker=marker_l, c='r')
            elif annee == 2003:
                marker_l = "v"
                ls_l = '-'
                plt.plot(mois, valeurs, label='%s' % annee, ls=ls_l,
                         marker=marker_l, c='b')
            # else:
            #     plt.plot(mois, valeurs, label='%s' % annee)

        plt.suptitle(u"Taux de mortalité pour %s période 1990 2020"
                     u"\nTaux Maximun : %s "
                     u"\nDate de génération : %s"
                     % (liste_departements[dep], date_max,
                        datetime.datetime.now().strftime("%d/%m/%Y")))

        moy_dc_a = np.array([x for x in moy_dc.values()])
        min_dc_a = np.array([x for x in min_dc.values()])
        max_dc_a = np.array([x for x in max_dc.values()])
        std_dc_a = np.array([x for x in std_l.values()])
        i = np.array(range(1, 13))
        axes = plt.gca()
        axes.set_ylim([0, 0.35])
        plt.plot(i, moy_dc_a, label='moyenne', ls='--', marker="v", c='m')

        plt.errorbar(i, moy_dc_a, yerr=std_dc_a, fmt='.k', label='ecart type')
        plt.plot(i, max_dc_a, label='maximum', ls='--', marker="+", c='k')
        plt.plot(i, min_dc_a, label='minimum', ls='-.', marker=(8, 2, 0),
                 c='k')
        plt.legend(loc=9)
        plt.show()
        f_out = os.path.join(repertoire_graphiques,
                             "deces_demographie_%s.jpeg" % dep)
        if os.path.exists(f_out):
            os.remove(f_out)
        fig.savefig(f_out)

except (Exception, mysql.connector.Error) as error:
    print('%s' % commande)
    tracage(traceback.print_exc())
    tracage("Erreur pendant la génération des images:  %s" % error)

finally:
    if mydb:
        curseur.close()
        mydb.close()
        tracage("Fermeture connection")
    tracage("Temps d'exécution:  %s " % (datetime.datetime.now() - debut))
