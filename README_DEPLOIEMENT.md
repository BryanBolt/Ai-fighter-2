# Mettre a jour AI Fighter 2 en ligne (GitHub + Render)

Ton service Render existe deja : https://ai-fighter-2-3zjq.onrender.com
Il est relie au depot GitHub **BryanBolt/Ai-fighter-2** (branche `main`).
Ce dossier a exactement la meme organisation que la racine du depot : tu n'as qu'a **remplacer les fichiers**.
Render se met a jour tout seul des que GitHub change.

## 1. Envoyer les fichiers sur GitHub (sans rien installer)
1. Va sur https://github.com/BryanBolt/Ai-fighter-2 et connecte-toi.
2. Clique **Add file > Upload files**.
3. Ouvre ce dossier sur ton PC, selectionne **tout son contenu** (`Dockerfile`, `render.yaml`, dossiers `server` et `web`, ce fichier) et fais-les glisser dans la page GitHub.
   - Si tu glisses les dossiers `server` et `web`, GitHub garde leur organisation et remplace les fichiers du meme nom
     (`web/ai_fighter_2.wasm`, `web/index.html`, `web/mq_js_bundle.js`, `server/server.js`...).
4. En bas, laisse "Commit directly to the main branch", puis clique **Commit changes**.

## 2. Verifier que Render a fini
1. Va sur https://dashboard.render.com et ouvre le service **ai-fighter-2**.
2. Onglet **Events** : une ligne "Deploy started" puis "Deploy live" (environ 2 a 5 minutes). Si tu vois "Deploy failed", ouvre **Logs** et envoie-moi les dernieres lignes.
3. Si rien ne demarre tout seul : bouton **Manual Deploy > Deploy latest commit**.

## 3. Jouer avec un ami (un seul lien)
- Ouvre https://ai-fighter-2-3zjq.onrender.com (PC, telephone, manette ou tactile).
- Le plus simple : **JOUER EN LIGNE** rejoint la partie publique deja en cours (ou en lance une), sans code ni menu. Tout le monde peut arriver en plein milieu ; **REJOINDRE UNE PARTIE** liste les parties en cours.
- Clique **JOUER AVEC UN AMI** : le jeu lance une partie et **copie le lien** (sur telephone, la fenetre de partage s'ouvre).
  Tu peux aussi appuyer sur **INVITER UN AMI** dans le salon.
- Envoie le lien (par exemple `https://ai-fighter-2-3zjq.onrender.com/?salon=K7M2P`) par SMS, WhatsApp, Discord...
- Ton ami clique : il arrive **directement dans ta partie, deja lancee** (une IA lui laisse sa place), sans menu ni pseudo a taper. Rien a lancer.
- Si le salon est complet, introuvable ou si la page est trop ancienne, le jeu l'explique (et propose de creer un salon).

## 3 bis. Jeu complet en ligne (version 3)
Mode histoire et Campagne deviennent des **parties ouvertes aux amis** : Jouer > mode > heros, la partie est listee dans PARTIES EN COURS (sauf **Partie privee** dans le menu Jouer) et le bouton **INVITER** (en jeu ou dans la pause) copie le lien. Options > **Jouer hors ligne** garde tout en local ; si le serveur ne repond pas en 8 s, le jeu se lance en local tout seul.
**A mettre en ligne apres cette mise a jour (les deux, sinon les parties completes ne marchent pas) :**
- `server/server.js` (protocole v3 : equipement et commandes sequencees) ;
- `web/` : `ai_fighter_2.wasm` (et `index.html`, `mq_js_bundle.js` s'ils ont change). Ancienne page + nouveau serveur : les parties de base marchent, pas les parties completes (message "mets le jeu a jour").
Les joueurs doivent recharger la page (Ctrl+F5) pour obtenir la version 3 ; le jeu PC/Android doit etre reconstruit pour rejoindre une partie complete.

## 4. Le serveur gratuit s'endort
Le plan gratuit de Render coupe le service apres 15 minutes sans visite ; le premier visiteur attend alors **30 a 60 secondes** (le jeu affiche "Reveil du serveur... 30 s" et reessaie tout seul).
Astuces gratuites :
- **Reveille-le toi-meme 1 minute avant de jouer** : ouvre simplement l'adresse du service dans ton navigateur.
- Un ping regulier (ex. un moniteur gratuit sur uptimerobot.com qui visite `/health` toutes les 10 minutes) evite l'endormissement. A creer toi-meme : je ne cree aucun compte.
- Option payante : plan **Starter, environ 7 $/mois** (Render > service > Settings > Instance Type) : le serveur ne dort plus jamais.
- Le disque est temporaire : les salons disparaissent si le serveur redemarre (sans consequence, on recree un salon).

## 5. Le jeu PC (.exe)
L'adresse du serveur est deja reglee sur `ai-fighter-2-3zjq.onrender.com` (constante `DEFAULT_SERVER` dans `src/net.rs`).
Pour un serveur de test : variable `AFH_SERVER=ws://127.0.0.1:8787`, ou le jeu : **En ligne > Options reseau**.
Le lien copie depuis le PC pointe vers la meme adresse (la version web s'ouvre chez l'ami).

## Contenu de ce dossier
`Dockerfile` et `render.yaml` (la recette Render, route de sante `/health`), `server/` (le relais, sans `node_modules` : Render les installe), `web/` (le jeu : page, chargeur et `ai_fighter_2.wasm`).
