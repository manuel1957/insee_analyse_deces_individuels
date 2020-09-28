# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0112, C0200
# pylint: disable=W0703
"""
Created on Sat Aug  1 12:37:22 2020.

@author: manuel

Objet: comparaison des ages de décès par mois de 2018 à 2020.
"""

import os
import codecs
import datetime
import traceback
import mysql.connector
# import pandas as pd
# from affichage_pyramide import affichage_pyramide
from matplotlib import pyplot
from outils_communs import tracage, connexion_mysql
from parametrage_application import repertoire_graphiques, repertoire_travail


# programme principal

fichier_traces = codecs.open(os.path.join(repertoire_travail, 'logs',
                                          'pyramide_ages_deces.txt'),
                             'a', 'UTF-8')
repertoire_pyramide = os.path.join(repertoire_graphiques, "Pyramides")
if not os.path.exists(repertoire_pyramide):
    os.makedirs(repertoire_pyramide)

debut = datetime.datetime.now()
tracage(u"Début de recherche", fichier_traces)

nb_maj = 0

# requete = """
# SELECT
#     SUM(IF(age < 60,1,0)) as '00 - 09',
#     SUM(IF(age >= 60, 1, 0)) as '> 120'
# FROM (SELECT age  FROM deces WHERE  date_deces >= '%s 00:00:00'
#       AND date_deces <= '%s 23:59:59') as donnees;
# """
requete = """
SELECT
    SUM(IF(age < 10,1,0)) as '00 - 09',
    SUM(IF(age BETWEEN 10 and 19,1,0)) as '10 - 19',
    SUM(IF(age BETWEEN 20 and 29,1,0)) as '20 - 29',
    SUM(IF(age BETWEEN 30 and 39,1,0)) as '30 - 39',
    SUM(IF(age BETWEEN 40 and 49,1,0)) as '40 - 49',
    SUM(IF(age BETWEEN 50 and 59,1,0)) as '50 - 59',
    SUM(IF(age BETWEEN 60 and 69,1,0)) as '60 - 69',
    SUM(IF(age BETWEEN 70 and 79,1,0)) as '70 - 79',
    SUM(IF(age BETWEEN 80 and 89,1,0)) as '80 - 89',
    SUM(IF(age BETWEEN 90 and 99,1,0)) as '90 - 99',
    SUM(IF(age BETWEEN 100 and 109,1,0)) as '100 - 109',
    SUM(IF(age >=110, 1, 0)) as '> 110'
FROM (SELECT age  FROM deces WHERE  date_deces >= '%s 00:00:00'
      AND date_deces <= '%s 23:59:59') as donnees;
"""

classes = ['00 - 09', '10 - 19', '20 - 29', '30 - 39', '40 - 49',
           '50 - 59', '60 - 69', '70 - 79', '80 - 89', '90 - 99',
           '100 - 109', '> 110']

classes_graphiques = ['Total'] + classes

try:
    # Connexion au serveur Mysql
    mabase, curseur = connexion_mysql()

    f_out_t = os.path.join(repertoire_pyramide, "pyramide_ages_deces.csv")
    f_out = open(f_out_t, 'w')
    annee_debut = 2018

    liste_dates = []
    pyramide_dic = {}
    for i in range(1, 13):
        pyramide_dic[i] = {}
    for an in range(annee_debut, 2021):
        for i in range(1, 13):
            pyramide_dic[i][an] = {}
            pyramide_dic[i][an]['tot'] = 0

    for an in range(annee_debut, 2021):
        for mois in range(1, 13):
            date_deb = datetime.date(an, mois, 1).strftime('%Y-%m-%d')
            if mois == 12:
                date_fin = datetime.date(an+1, 1, 1)
            else:
                date_fin = datetime.date(an, mois + 1, 1)
            date_fin = date_fin - datetime.timedelta(days=1)
            date_fin = date_fin.strftime('%Y-%m-%d')

            commande = requete % (date_deb, date_fin)
            curseur.execute(commande)
            tracage('fin du select %s ' % date_deb, fichier_traces)

            pyramide = curseur.fetchall()
            ligne = ""
            # print(pyramide)
            for i in pyramide:
                if i[0] is None:
                    for j in range(0, len(classes)):
                        pyramide_dic[mois][an][classes[j]] = 0

                else:
                    for j in range(0, len(classes)):
                        pyramide_dic[mois][an][classes[j]] = int(i[j])
                        pyramide_dic[mois][an]['tot'] += int(i[j])

    # génération du fichier pour tous les mois de 01/annee_debut à 12/2020
    f_out.write('mois\tannee\tTotal\t')
    for j in range(0, len(classes)):
        f_out.write('%s\t' % classes[j])
    f_out.write('\n')

    for mois in range(1, 13):
        for an in range(annee_debut, 2021):
            f_out.write('%s\t%s\t%s\t' % (mois, an,
                                          pyramide_dic[mois][an]['tot']))
            for j in range(0, len(classes)):
                f_out.write('%s\t' % pyramide_dic[mois][an][classes[j]])
            f_out.write('\n')
    f_out.close()

    # comparaison graphique des données entre 2018/01 et 2020/12
    for mois in range(1, 13):
        # for an in range(annee_debut, 2020):
        print("traite %s %s" % (an, mois))
        barWidth = 0.3
        y1 = list(pyramide_dic[mois][2018].values())
        y2 = list(pyramide_dic[mois][2019].values())
        y3 = list(pyramide_dic[mois][2020].values())
        r1 = range(len(y1))
        r2 = [x + barWidth for x in r1]
        r3 = [x + barWidth for x in r2]
        fig, ax = pyplot.subplots()

        pyplot.axis([0.0, len(classes_graphiques), 0, 70000])
        ax = pyplot.gca()
        ax.set_autoscale_on(False)
        pyplot.grid(b=True, which='both', axis='both')

        ax.set_xticks(range(len(classes_graphiques)))
        ax.set_xticklabels(classes_graphiques, rotation=30)
        ax.bar(r1, y1, width=barWidth, color=['blue' for i in y1],
               label="2018 %02i" % mois)
        ax.bar(r2, y2, width=barWidth, color=['yellow' for i in y1],
               label="2019 %02i" % mois)
        ax.bar(r3, y3, width=barWidth, color=['red' for i in y1],
               label="2020 %02i" % mois)

        titre = 'Comparaison 2018 2019 2020 pour le mois %s' % mois
        pyplot.title(titre)

        legend = ax.legend(loc='upper center', shadow=True,
                           fontsize='medium')

        pyplot.show()
        f_out = os.path.join(repertoire_pyramide,
                             "deces_comparaison_2018_2019_2020_%s.png" %
                             ("%02i" % mois))
        if os.path.exists(f_out):
            os.remove(f_out)
        fig.savefig(f_out, dpi=200)
        pyplot.close()


except (Exception, mysql.connector.Error) as error:
    tracage(traceback.print_exc(), fichier_traces)
    tracage("Erreur pendant la génération de la pyramide:  %s" % error,
            fichier_traces)

finally:

    if mabase:
        curseur.close()
        mabase.close()
        tracage("Fermeture connection", fichier_traces)
    tracage("Temps d'exécution:  %s " % (datetime.datetime.now() - debut),
            fichier_traces)
