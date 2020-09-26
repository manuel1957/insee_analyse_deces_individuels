# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0112
# pylint: disable=W0703, R1721
"""
Created on Sat Aug  1 12:37:22 2020.

@author: manuel

Objet: extraction des données de décès de la base regroupées par
département année mois et génération des fichiers csv contenant ces
données et génération des fichier images afférants.
Nécessite les fichiers de démographie départementales générés précedement.

"""


import os
import codecs
import datetime
import traceback
from PIL import Image

import mysql.connector
import matplotlib.pyplot as plt
from PyPDF2 import PdfFileMerger, PdfFileReader

import numpy as np
from outils_communs import tracage, departements, liste_departements,\
    connexion_mysql
from parametrage_application import repertoire_travail, repertoire_graphiques,\
    repertoire_demographie


# programme principal
date_du_jour = datetime.datetime.now().strftime("%Y_%m_%d")
heure_date_du_jour = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M")
annee_en_cours = datetime.datetime.now().year
annee_debut = 1990

repertoire_logs = os.path.join(repertoire_travail, "logs")
if not os.path.exists(repertoire_logs):
    os.makedirs(repertoire_logs)

fichier_traces = \
    os.path.join(repertoire_logs, 'recherche_deces_dep_%s.txt' %
                 heure_date_du_jour)
fichier_traces = codecs.open(fichier_traces, 'a', 'UTF-8')

debut = datetime.datetime.now()
tracage(u"Début de recherche", fichier_traces)


repertoire_graphiques = os.path.join(repertoire_graphiques,
                                     "Graphiques_%s" % date_du_jour)

if not os.path.exists(repertoire_graphiques):
    os.makedirs(repertoire_graphiques)


try:

    # Connexion au serveur Mysql
    mabase, curseur = connexion_mysql()

    #  pour normaliser les schémas ont retient le taux max de décès
    max_taux_deces_tot = 0.35

    # boucle sur les départements extraction des données
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

        tracage('fin du select dep %s ' % dep, fichier_traces)
        liste_dep = curseur.fetchall()
        f_out_t = os.path.join(repertoire_graphiques,
                               "deces_vs_demographie_%s.csv" % dep)
        f_out = codecs.open(f_out_t, 'w', 'UTF-8')
        f_out.write(u"Année \tMois \tNb décès \tNb habitants"
                    u" \tTaux mortalité\n")

        # lecture du fichier démographie du département
        table_demo = {}
        fichier_demo = os.path.join(repertoire_demographie,
                                    "demographie_dep_%s.csv" % dep)
        if not os.path.exists(fichier_demo):
            tracage("Le fichier demographie du département %s n'existe pas" %
                    dep, fichier_traces)
            raise("Le fichier demographie du département %s n'existe pas" %
                  dep)

        fichier_demog = open(fichier_demo)
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

        donnees = {}
        max_dc = {}
        min_dc = {}
        moy_dc = {}
        maximum = 0
        taux_max_dep_date = ""
        for i in range(1, 13):
            max_dc[i] = 0
            min_dc[i] = 9e+6
            moy_dc[i] = 0

        nb_annees = 0
        for annee, mois, deces in liste_dep:
            # pour l'avoir en pourcentage on multiplie le nombre de décès
            # par 100
            deces = deces * 100

            if annee < annee_debut - 1 or annee not in table_demo or \
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
            if annee < annee_en_cours:
                if donnee_redressee > max_dc[mois]:
                    max_dc[mois] = donnee_redressee
                    if donnee_redressee > maximum:
                        maximum = donnee_redressee
                        taux_max_dep_date = "%.2f %% en date du %s/%s :" % \
                            (maximum, mois, annee)

                if donnee_redressee < min_dc[mois]:
                    min_dc[mois] = donnee_redressee
                moy_dc[mois] += donnee_redressee
        for i in range(1, 13):
            if nb_annees != 0:
                moy_dc[i] = moy_dc[i] / nb_annees
        f_out.close()
        # fin d'écriutre dans le fichier csv

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

        for annee in donnees:

            mois = np.array([x for x in donnees[annee].keys()])
            valeurs = np.array([x for x in donnees[annee].values()])

            if annee == annee_en_cours:
                marker_l = "o"
                ls_l = ':'
                plt.plot(mois, valeurs, label='%s' % annee, ls=ls_l,
                         marker=marker_l, c='r')
            elif annee == 2003:
                marker_l = "v"
                ls_l = '-'
                plt.plot(mois, valeurs, label='%s' % annee, ls=ls_l,
                         marker=marker_l, c='b')

        moy_dc_a = np.array([x for x in moy_dc.values()])
        min_dc_a = np.array([x for x in min_dc.values()])
        max_dc_a = np.array([x for x in max_dc.values()])
        std_dc_a = np.array([x for x in std_l.values()])
        i = np.array(range(1, 13))
        axes = plt.gca()
        axes.set_ylim([0, max_taux_deces_tot])
        plt.plot(i, moy_dc_a, label='moyenne', ls='--', marker="v", c='m')

        plt.errorbar(i, moy_dc_a, yerr=std_dc_a, fmt='.k', label='ecart type')
        plt.plot(i, max_dc_a, label='maximum', ls='--', marker="+", c='k')
        plt.plot(i, min_dc_a, label='minimum', ls='-.', marker=(8, 2, 0),
                 c='k')

        plt.suptitle(u"Taux de mortalité pour %s période de %s à %s"
                     u"\nTaux Maximun de décès pour le département : %s "
                     u"\nDate de génération : %s"
                     % (liste_departements[dep], annee_debut, annee_en_cours,
                        taux_max_dep_date,
                        datetime.datetime.now().strftime("%d/%m/%Y")))
        plt.legend(loc=9)
        plt.show()
        f_out = os.path.join(repertoire_graphiques,
                             "deces_demographie_%s.jpeg" % dep)
        if os.path.exists(f_out):
            os.remove(f_out)
        fig.savefig(f_out)
        plt.close()

    # Génération du fichier pdf récapitulatif
    liste_images = []
    image_base = ""

    for image in os.listdir(repertoire_graphiques):
        if image.find('.jpeg') != -1:
            nom_fichier_image = os.path.join(repertoire_graphiques, image)
            if image.find('_FR') == -1:

                liste_images.append(Image.open(nom_fichier_image))
            else:
                image_base = Image.open(nom_fichier_image)

    pdf1_filename = os.path.join(repertoire_graphiques,
                                 "covid mortalité par département.pdf")
    image_base.save(pdf1_filename, "PDF", resolution=100.0,
                    save_all=True, append_images=liste_images)

    merger = PdfFileMerger()
    fichier_entete = os.path.join("fichiers_utiles",
                                  "présentation des graphiques normalisés.pdf")
    merger.append(PdfFileReader(fichier_entete))

    merger.append(PdfFileReader(pdf1_filename))

    merger.write(pdf1_filename)
    merger.close()

except (Exception, mysql.connector.Error) as error:
    print('%s' % commande)
    tracage(traceback.print_exc(), fichier_traces)
    tracage("Erreur pendant la génération des images:  %s" % error,
            fichier_traces)

finally:
    # si mabase existe il faut la fermer et rendre le curseur ouvert
    if mabase:
        curseur.close()
        mabase.close()
        tracage("Fermeture connexion", fichier_traces)
    tracage("Temps d'exécution:  %s " % (datetime.datetime.now() - debut),
            fichier_traces)
