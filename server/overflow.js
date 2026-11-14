// Corps trop gros : on repond 413 PUIS on jette le reste du corps en le lisant (borne), au lieu de couper la connexion.
// Couper (req.destroy / connection: close) avec des octets non lus fait partir un RST qui peut effacer la reponse 413
// cote client (fetch : « fetch failed » au lieu de 413), surtout sous charge ou sous Windows : tests et clients instables.
export const DROP_CAP = 8 * 1024 * 1024;
export function dropBody(req, cap = DROP_CAP) {
  req.removeAllListeners('data');
  let n = 0;
  req.on('data', (c) => { n += c.length; if (n > cap) req.destroy(); });
  req.resume();
}
