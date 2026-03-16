require('dotenv').config();
const express  = require('express');
const session  = require('express-session');
const mongoose = require('mongoose');
const https    = require('https');
const path     = require('path');
const crypto   = require('crypto');
const http     = require('http');
const socketIo = require('socket.io');

const app    = express();
const server = http.createServer(app);
const io     = socketIo(server, { cors: { origin: '*' } });

global.io = io;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use('/backgrounds', express.static(path.join(__dirname, 'public', 'backgrounds')));

app.use(session({
  secret: process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex'),
  resave: false, saveUninitialized: false,
  cookie: { secure: false, maxAge: 24 * 60 * 60 * 1000 }
}));

mongoose.connect(process.env.MONGO_URI).then(() => console.log('[DB] Conectado')).catch(console.error);

// ══════════════════════════════════════════════════════════════════════════
//  SCHEMAS — exactamente los mismos que usa el bot de Discord
//  Las monedas se guardan en UserCoins, que es compartido con el bot
// ══════════════════════════════════════════════════════════════════════════
const { Schema, model } = mongoose;
const Level           = model('Level',           new Schema({ userId: String, guildId: String, xp: { type: Number, default: 0 }, level: { type: Number, default: 0 }, lastMessage: Date, streak: { type: Number, default: 0 }, lastMessageDate: Date, dailyChallenges: { date: String, messages: Number, messagesCompleted: Boolean, vcMinutes: Number, vcCompleted: Boolean } }));
const Warn            = model('Warn',            new Schema({ userId: String, guildId: String, moderator: String, reason: String, createdAt: { type: Date, default: Date.now } }));
const Mute            = model('Mute',            new Schema({ userId: String, guildId: String, expiresAt: Date, reason: String }));
const Giveaway        = model('Giveaway',        new Schema({ guildId: String, channelId: String, messageId: String, prize: String, winners: Number, hostedBy: String, endsAt: Date, ended: Boolean, participants: [String], winnerIds: [String], requiredRole: String, minLevel: Number, minInvites: Number, claimDeadline: Date, claimedBy: [String] }));
const GiveawayBlacklist = model('GiveawayBlacklist', new Schema({ guildId: String, userId: String, reason: { type: String, default: 'Sin razón' }, addedBy: String, addedAt: { type: Date, default: Date.now } }));
const GiveawayConfig  = model('GiveawayConfig',  new Schema({ guildId: { type: String, unique: true }, claimTimeout: { type: Number, default: 86400000 } }));
const AutoRole        = model('AutoRole',        new Schema({ guildId: String, panelId: { type: String, unique: true }, channelId: String, messageId: String, name: String, title: String, description: String, requireConfirm: { type: Boolean, default: true }, maxRoles: { type: Number, default: 0 }, roles: [{ roleId: String, emoji: String, label: String, description: String, order: Number, minLevel: { type: Number, default: 0 }, requiredRole: String }], createdAt: { type: Date, default: Date.now }, updatedAt: { type: Date, default: Date.now } }));
const SecurityLog     = model('SecurityLog',     new Schema({ userId: String, guildId: String, ip: String, country: String, countryCode: String, action: String, flagged: Boolean, joinedAt: { type: Date, default: Date.now } }));
const InviteTracker   = model('InviteTracker',   new Schema({ guildId: String, userId: String, code: String, uses: { type: Number, default: 0 }, total: { type: Number, default: 0 }, fake: { type: Number, default: 0 }, left: { type: Number, default: 0 } }));

// ── UserCoins: compartido con el bot — modificar aquí afecta el balance real
const UserCoins       = model('UserCoins',       new Schema({ userId: String, guildId: String, coins: { type: Number, default: 0 }, totalEarned: { type: Number, default: 0 }, lastDaily: Date, lastWork: Date, ownedBackgrounds: { type: [String], default: [] }, activeBgId: { type: String, default: null } }));

const Loan            = model('Loan',            new Schema({ userId: String, guildId: String, amount: Number, debt: Number, interestRate: { type: Number, default: 0.01 }, status: { type: String, default: 'active' }, lastInterest: Date, createdAt: { type: Date, default: Date.now } }));
const ChannelsConfig  = model('ChannelsConfig',  new Schema({ guildId: String, logs: String, modLogs: String, welcome: String, levels: String, verification: String, tiktok: String, security: String, invites: String }));
const RewardsConfig   = model('RewardsConfig',   new Schema({ guildId: String, initialCoins: Number, trivia_easy: Number, trivia_medium: Number, trivia_hard: Number, misiones_reward: Number, shop_rol_custom: Number, shop_mention: Number, shop_vc_boost: Number, max_loss_per_day: Number, max_bets_per_hour: Number, logsChannelId: String }));
const GuildStyle      = model('GuildStyle',      new Schema({ guildId: { type: String, unique: true }, colors: { primary: Number, success: Number, warning: Number, danger: Number, info: Number, welcome: Number, giveaway: Number, level: Number }, footer: { text: String, iconUrl: String }, welcomeMessage: String, showTimestamp: { type: Boolean, default: true } }));
const CasinoLog       = model('CasinoLog',       new Schema({ userId: String, guildId: String, game: String, bet: Number, result: String, profit: Number, timestamp: { type: Date, default: Date.now } }));
const Achievements    = model('Achievements',    new Schema({ userId: String, guildId: String, badges: [{ id: String, name: String, unlockedAt: Date }], trivia_wins: { type: Number, default: 0 }, casino_wins: { type: Number, default: 0 }, win_streak: { type: Number, default: 0 }, max_bet: { type: Number, default: 0 }, createdAt: { type: Date, default: Date.now } }));
const MissionProgress = model('MissionProgress', new Schema({ userId: String, guildId: String, week: Number, year: Number, missions: [{ id: String, name: String, target: Number, progress: { type: Number, default: 0 }, completed: { type: Boolean, default: false }, reward: { type: Number, default: 300 } }], claimed: { type: Boolean, default: false }, claimedAt: Date, createdAt: { type: Date, default: Date.now } }));
const TriviaProgress  = model('TriviaProgress',  new Schema({ userId: String, guildId: String, totalWins: { type: Number, default: 0 }, totalFails: { type: Number, default: 0 }, streak: { type: Number, default: 0 }, triviaCount: { type: Number, default: 0 }, createdAt: { type: Date, default: Date.now } }));
const Trade           = model('Trade',           new Schema({ guildId: String, fromUserId: String, toUserId: String, fromCoins: { type: Number, default: 0 }, toCoins: { type: Number, default: 0 }, status: { type: String, default: 'pending' }, expiresAt: Date, createdAt: { type: Date, default: Date.now } }));
const ShopItem        = model('ShopItem',        new Schema({ guildId: String, itemId: String, name: String, description: String, price: Number, emoji: { type: String, default: '🛒' }, active: { type: Boolean, default: true }, createdBy: String, createdAt: { type: Date, default: Date.now } }));
const XpBoost         = model('XpBoost',         new Schema({ userId: String, guildId: String, multiplier: Number, expiresAt: Date, grantedBy: String }));
const GameHistory     = model('GameHistory',     new Schema({ guildId: String, game: String, result: mongoose.Schema.Types.Mixed, players: [{ userId: String, betType: String, amount: Number, won: Boolean, delta: Number }], endedAt: { type: Date, default: Date.now } }));
const ServerBackground= model('ServerBackground',new Schema({ guildId: String, bgId: String, name: String, description: String, price: { type: Number, default: 0 }, filePath: String, isDefault: { type: Boolean, default: false }, createdBy: String, createdAt: { type: Date, default: Date.now } }));
const Arm             = model('Arm',             new Schema({ userId: String, guildId: String, itemId: String, name: String, skin: String, type: String, emoji: String, rarity: String, rarityName: String, rarityEmoji: String, rarityColor: Number, wear: String, wearTag: String, basePrice: Number, sellPrice: Number, caseId: String, tradeId: String, obtainedAt: { type: Date, default: Date.now } }));
const MarketListing   = model('MarketListing',   new Schema({ guildId: String, sellerId: String, armId: mongoose.Schema.Types.ObjectId, itemId: String, name: String, skin: String, type: String, rarity: String, rarityName: String, rarityColor: Number, wear: String, wearTag: String, basePrice: Number, price: Number, status: { type: String, default: 'active' }, buyerId: String, listedAt: { type: Date, default: Date.now }, expiresAt: Date, soldAt: Date }));
const ArmCurrentPrice = model('ArmCurrentPrice', new Schema({ guildId: String, itemId: String, name: String, skin: String, rarity: String, rarityColor: Number, currentPrice: Number, openPrice24h: Number, high24h: Number, low24h: Number, volume24h: { type: Number, default: 0 }, lastUpdated: Date }));
const ArmPrice        = model('ArmPrice',        new Schema({ guildId: String, itemId: String, name: String, skin: String, rarity: String, open: Number, high: Number, low: Number, close: Number, volume: Number, timestamp: { type: Date, default: Date.now } }));
const MarketTx        = model('MarketTx',        new Schema({ guildId: String, itemId: String, name: String, skin: String, rarity: String, price: Number, sellerId: String, buyerId: String, timestamp: { type: Date, default: Date.now } }));
const ShopPurchase    = mongoose.models.ShopPurchase || model('ShopPurchase', new Schema({ userId: String, guildId: String, itemId: String, itemName: String, price: Number, purchasedAt: { type: Date, default: Date.now } }));

// ══════════════════════════════════════════════════════════════════════════
//  CONFIG
// ══════════════════════════════════════════════════════════════════════════
const IP_LOG_ALLOWED = ['990868869449121852', '751153328326443109'];
const GUILD_ID       = process.env.GUILD_ID;
const ADMIN_ROLE_IDS = (process.env.ADMIN_ROLE_IDS || process.env.ADMIN_ROLE_ID || '').split(',').map(r => r.trim()).filter(Boolean);
const userCache      = new Map();

// ══════════════════════════════════════════════════════════════════════════
//  HELPERS DISCORD
// ══════════════════════════════════════════════════════════════════════════
function discordReq(endpoint, bearer, isBot) {
  return new Promise(resolve => {
    const req = https.request({
      hostname: 'discord.com', path: `/api/v10${endpoint}`, method: 'GET',
      headers: { Authorization: isBot ? `Bot ${process.env.DISCORD_TOKEN}` : `Bearer ${bearer}` }
    }, res => {
      let b = '';
      res.on('data', c => b += c);
      res.on('end', () => { try { resolve(JSON.parse(b)); } catch { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.end();
  });
}

async function getUser(userId) {
  if (userCache.has(userId)) return userCache.get(userId);
  const u = await discordReq(`/users/${userId}`, null, true);
  const data = {
    username: u?.username || userId,
    avatar: u?.avatar ? `https://cdn.discordapp.com/avatars/${userId}/${u.avatar}.png?size=32` : null
  };
  userCache.set(userId, data);
  return data;
}

function exchangeCode(code) {
  return new Promise(resolve => {
    const params = new URLSearchParams({
      client_id: process.env.DISCORD_CLIENT_ID,
      client_secret: process.env.DISCORD_CLIENT_SECRET,
      grant_type: 'authorization_code',
      code,
      redirect_uri: process.env.DISCORD_REDIRECT_URI
    }).toString();
    const req = https.request({
      hostname: 'discord.com', path: '/api/v10/oauth2/token', method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(params) }
    }, res => {
      let b = '';
      res.on('data', c => b += c);
      res.on('end', () => { try { resolve(JSON.parse(b)); } catch { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.write(params);
    req.end();
  });
}

const requireAuth = (req, res, next) => req.session?.user ? next() : res.status(401).json({ error: 'No autenticado' });

// ══════════════════════════════════════════════════════════════════════════
//  AUTH
// ══════════════════════════════════════════════════════════════════════════
app.get('/auth/discord', (req, res) => {
  if (req.query.from) req.session.casinoFrom = req.query.from;
  res.redirect(`https://discord.com/oauth2/authorize?client_id=${process.env.DISCORD_CLIENT_ID}&redirect_uri=${encodeURIComponent(process.env.DISCORD_REDIRECT_URI)}&response_type=code&scope=identify`);
});

app.get('/auth/callback', async (req, res) => {
  const { code } = req.query;
  if (!code) return res.redirect('/login?error=no_code');
  try {
    const tokens = await exchangeCode(code);
    if (!tokens?.access_token) return res.redirect('/login?error=token_failed');
    const user   = await discordReq('/users/@me', tokens.access_token, false);
    if (!user?.id) return res.redirect('/login?error=user_failed');
    const member = await discordReq(`/guilds/${GUILD_ID}/members/${user.id}`, null, true);
    const isAdmin = member?.roles ? member.roles.some(r => ADMIN_ROLE_IDS.includes(r)) : false;
    req.session.user = {
      id: user.id,
      username: user.username,
      avatar: user.avatar
        ? `https://cdn.discordapp.com/avatars/${user.id}/${user.avatar}.png?size=64`
        : 'https://cdn.discordapp.com/embed/avatars/0.png',
      isAdmin,
    };
    const from = req.session.casinoFrom;
    req.session.casinoFrom = null;
    if (from === 'casino') return res.redirect('/casino');
    if (!member?.roles) return res.redirect('/login?error=not_in_server');
    if (!isAdmin) return res.redirect('/login?error=no_permission');
    res.redirect('/dashboard');
  } catch (e) { console.error('[AUTH]', e); res.redirect('/login?error=server_error'); }
});

app.get('/auth/logout', (req, res) => { req.session.destroy(); res.redirect('/login'); });
app.get('/api/me', (req, res) => req.session?.user ? res.json(req.session.user) : res.status(401).json({ error: 'No autenticado' }));

// ══════════════════════════════════════════════════════════════════════════
//  STATS HELPER
// ══════════════════════════════════════════════════════════════════════════
async function getStats() {
  try {
    const [guild, totalUsers, activeGiveaways, activeLoans, flaggedJoins, recentLogs, casinoAgg, totalWarns] = await Promise.all([
      discordReq(`/guilds/${GUILD_ID}?with_counts=true`, null, true),
      UserCoins.countDocuments({ guildId: GUILD_ID }),
      Giveaway.countDocuments({ guildId: GUILD_ID, ended: false }),
      Loan.countDocuments({ guildId: GUILD_ID, status: 'active' }),
      SecurityLog.countDocuments({ guildId: GUILD_ID, flagged: true }),
      SecurityLog.find({ guildId: GUILD_ID }).sort({ joinedAt: -1 }).limit(60).lean(),
      CasinoLog.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: null, bets: { $sum: 1 }, profit: { $sum: '$profit' } } }]),
      Warn.countDocuments({ guildId: GUILD_ID }),
    ]);
    const joinsByDay = {};
    for (let i = 6; i >= 0; i--) {
      const d = new Date(); d.setDate(d.getDate() - i);
      joinsByDay[d.toLocaleDateString('es-PE', { weekday: 'short', day: 'numeric' })] = 0;
    }
    recentLogs.forEach(l => {
      const key = new Date(l.joinedAt).toLocaleDateString('es-PE', { weekday: 'short', day: 'numeric' });
      if (joinsByDay[key] !== undefined) joinsByDay[key]++;
    });
    return {
      members: guild?.approximate_member_count || 0,
      online: guild?.approximate_presence_count || 0,
      registeredUsers: totalUsers, activeGiveaways, activeLoans, flaggedJoins, totalWarns,
      joinsByDay, casinoBets: casinoAgg[0]?.bets || 0, casinoProfit: casinoAgg[0]?.profit || 0
    };
  } catch (e) { console.error('[stats]', e); return null; }
}

// ══════════════════════════════════════════════════════════════════════════
//  HELPER: Actualizar monedas y notificar al bot via Socket + DB
//  Esto garantiza que el bot de Discord vea el cambio inmediatamente
// ══════════════════════════════════════════════════════════════════════════
async function applyCoins(userId, delta, isWin = false) {
  const update = { $inc: { coins: delta } };
  if (isWin && delta > 0) update.$inc.totalEarned = delta;
  const updated = await UserCoins.findOneAndUpdate(
    { userId, guildId: GUILD_ID },
    update,
    { new: true }
  );
  if (updated && global.io) {
    // Notifica al frontend del casino (actualiza saldo en tiempo real)
    global.io.emit('coins:update', { userId, coins: updated.coins });
    // Notifica al bot de Discord si está escuchando el mismo socket
    global.io.emit('bot:coins:update', { userId, guildId: GUILD_ID, coins: updated.coins, delta });
  }
  return updated;
}

async function logAndAchieve(userId, game, bet, won, delta) {
  await CasinoLog.create({ userId, guildId: GUILD_ID, game, bet, result: won ? 'win' : 'loss', profit: delta });
  if (won) {
    await Achievements.findOneAndUpdate(
      { userId, guildId: GUILD_ID },
      { $inc: { casino_wins: 1 }, $max: { max_bet: bet } },
      { upsert: true }
    );
  }
  // Emitir al feed en vivo del casino
  if (global.io) {
    const u = await getUser(userId);
    global.io.emit('casino:bet', { userId, username: u.username, game, bet, profit: delta });
  }
}

// ══════════════════════════════════════════════════════════════════════════
//  APIS DEL DASHBOARD (requieren rol admin)
// ══════════════════════════════════════════════════════════════════════════
app.get('/api/stats', requireAuth, async (req, res) => {
  const stats = await getStats();
  if (!stats) return res.status(500).json({ error: 'Error' });
  res.json(stats);
});

app.get('/api/invites', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1, limit = 15;
    const [agg, countAgg] = await Promise.all([
      InviteTracker.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: '$userId', reales: { $sum: '$uses' }, total: { $sum: '$total' }, fake: { $sum: '$fake' }, left: { $sum: '$left' } } }, { $sort: { reales: -1 } }, { $skip: (page-1)*limit }, { $limit: limit }]),
      InviteTracker.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: '$userId' } }, { $count: 'total' }]),
    ]);
    const enriched = await Promise.all(agg.map(async (inv, i) => { const u = await getUser(inv._id); return { rank: (page-1)*limit+i+1, userId: inv._id, username: u.username, avatar: u.avatar, reales: inv.reales, total: inv.total, fake: inv.fake, left: inv.left }; }));
    res.json({ invites: enriched, pages: Math.ceil((countAgg[0]?.total || 0) / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/giveaways', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1, status = req.query.status || 'all', limit = 10;
    const query = { guildId: GUILD_ID };
    if (status === 'active') query.ended = false;
    if (status === 'ended') query.ended = true;
    const [agg, count] = await Promise.all([Giveaway.find(query).sort({ endsAt: -1 }).skip((page-1)*limit).limit(limit).lean(), Giveaway.countDocuments(query)]);
    const enriched = await Promise.all(agg.map(async g => { const u = await getUser(g.hostedBy); return { ...g, hostedByName: u.username }; }));
    res.json({ giveaways: enriched, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/economy', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1, sort = req.query.sort || 'coins', limit = 15;
    if (sort === 'backgrounds') {
      const [agg, count] = await Promise.all([
        UserCoins.aggregate([{ $match: { guildId: GUILD_ID, 'ownedBackgrounds.0': { $exists: true } } }, { $addFields: { bgCount: { $size: '$ownedBackgrounds' } } }, { $sort: { bgCount: -1 } }, { $skip: (page-1)*limit }, { $limit: limit }]),
        UserCoins.countDocuments({ guildId: GUILD_ID, 'ownedBackgrounds.0': { $exists: true } }),
      ]);
      const enriched = await Promise.all(agg.map(async (u, i) => { const user = await getUser(u.userId); return { rank: (page-1)*limit+i+1, userId: u.userId, username: user.username, avatar: user.avatar, coins: u.coins, totalEarned: u.totalEarned, ownedBackgrounds: u.ownedBackgrounds || [], activeBgId: u.activeBgId || null, level: 0, xp: 0 }; }));
      return res.json({ users: enriched, pages: Math.ceil(count / limit) });
    }
    const sortObj = sort === 'level' ? { level: -1, xp: -1 } : { coins: -1 };
    const [agg, count] = await Promise.all([UserCoins.find({ guildId: GUILD_ID }).sort(sortObj).skip((page-1)*limit).limit(limit).lean(), UserCoins.countDocuments({ guildId: GUILD_ID })]);
    const enriched = await Promise.all(agg.map(async (u, i) => { const user = await getUser(u.userId); const lvl = await Level.findOne({ userId: u.userId, guildId: GUILD_ID }).lean(); return { rank: (page-1)*limit+i+1, userId: u.userId, username: user.username, avatar: user.avatar, coins: u.coins, totalEarned: u.totalEarned, ownedBackgrounds: u.ownedBackgrounds || [], activeBgId: u.activeBgId || null, level: lvl?.level || 0, xp: lvl?.xp || 0 }; }));
    res.json({ users: enriched, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/loans', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1, status = req.query.status || 'all', limit = 15;
    const query = { guildId: GUILD_ID }; if (status !== 'all') query.status = status;
    const [agg, count] = await Promise.all([Loan.find(query).sort({ createdAt: -1 }).skip((page-1)*limit).limit(limit).lean(), Loan.countDocuments(query)]);
    const enriched = await Promise.all(agg.map(async l => { const u = await getUser(l.userId); return { ...l, username: u.username, avatar: u.avatar }; }));
    res.json({ loans: enriched, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/levels', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1, limit = 15;
    const [agg, count, dist] = await Promise.all([
      Level.find({ guildId: GUILD_ID }).sort({ level: -1, xp: -1 }).skip((page-1)*limit).limit(limit).lean(),
      Level.countDocuments({ guildId: GUILD_ID }),
      Level.aggregate([{ $match: { guildId: GUILD_ID } }, { $bucket: { groupBy: '$level', boundaries: [0,5,10,20,50,999], default: '50+', output: { count: { $sum: 1 } } } }]),
    ]);
    const enriched = await Promise.all(agg.map(async (l, i) => { const u = await getUser(l.userId); return { rank: (page-1)*limit+i+1, userId: l.userId, username: u.username, avatar: u.avatar, level: l.level, xp: l.xp, streak: l.streak || 0, dailyChallenges: l.dailyChallenges || null }; }));
    const distribution = dist.map(d => ({ _id: d._id === 0 ? '0' : d._id === 5 ? '1–4' : d._id === 10 ? '5–9' : d._id === 20 ? '10–19' : d._id === 50 ? '20–49' : '50+', count: d.count }));
    res.json({ users: enriched, pages: Math.ceil(count / limit), total: count, distribution });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/moderation', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1, tab = req.query.tab || 'warns', limit = 15;
    if (tab === 'logs' && !IP_LOG_ALLOWED.includes(req.session.user.id)) {
      return res.status(403).json({ error: 'No tienes permiso para ver los logs de IP' });
    }
    let items = [], count = 0;
    if (tab === 'warns') {
      const agg = await Warn.find({ guildId: GUILD_ID }).sort({ createdAt: -1 }).skip((page-1)*limit).limit(limit).lean();
      items = await Promise.all(agg.map(async w => { const u = await getUser(w.userId); return { ...w, username: u.username, avatar: u.avatar, modName: w.moderator }; }));
      count = await Warn.countDocuments({ guildId: GUILD_ID });
    } else if (tab === 'mutes') {
      const agg = await Mute.find({ guildId: GUILD_ID }).sort({ _id: -1 }).skip((page-1)*limit).limit(limit).lean();
      items = await Promise.all(agg.map(async m => { const u = await getUser(m.userId); return { ...m, username: u.username, avatar: u.avatar }; }));
      count = await Mute.countDocuments({ guildId: GUILD_ID });
    } else if (tab === 'logs') {
      const agg = await SecurityLog.find({ guildId: GUILD_ID }).sort({ joinedAt: -1 }).skip((page-1)*limit).limit(limit).lean();
      items = await Promise.all(agg.map(async l => { const u = await getUser(l.userId); return { ...l, username: u.username, avatar: u.avatar }; }));
      count = await SecurityLog.countDocuments({ guildId: GUILD_ID });
    }
    res.json({ items, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/bans', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1, limit = 15;
    const query = { guildId: GUILD_ID, reason: { $regex: 'ban', $options: 'i' } };
    const [bans, count] = await Promise.all([Warn.find(query).sort({ createdAt: -1 }).skip((page-1)*limit).limit(limit).lean(), Warn.countDocuments(query)]);
    const enriched = await Promise.all(bans.map(async b => { const u = await getUser(b.userId); return { ...b, username: u.username, avatar: u.avatar }; }));
    res.json({ bans: enriched, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/casino', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1, game = req.query.game || 'all', limit = 15;
    const query = { guildId: GUILD_ID };
    if (game !== 'all') query.game = new RegExp('^' + game + '$', 'i');
    const [logs, count, topWinners, topLosers, statsAgg] = await Promise.all([
      CasinoLog.find(query).sort({ timestamp: -1 }).skip((page-1)*limit).limit(limit).lean(),
      CasinoLog.countDocuments(query),
      CasinoLog.aggregate([{ $match: { guildId: GUILD_ID, profit: { $gt: 0 } } }, { $group: { _id: '$userId', totalProfit: { $sum: '$profit' }, count: { $sum: 1 } } }, { $sort: { totalProfit: -1 } }, { $limit: 5 }]),
      CasinoLog.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: '$userId', netProfit: { $sum: '$profit' } } }, { $sort: { netProfit: 1 } }, { $limit: 5 }]),
      CasinoLog.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: null, totalBets: { $sum: 1 }, totalBetAmount: { $sum: '$bet' }, houseProfit: { $sum: '$profit' } } }]),
    ]);
    const enrichedLogs    = await Promise.all(logs.map(async l => { const u = await getUser(l.userId); return { ...l, username: u.username, avatar: u.avatar }; }));
    const enrichedWinners = await Promise.all(topWinners.map(async w => { const u = await getUser(w._id); return { ...w, username: u.username, avatar: u.avatar }; }));
    const enrichedLosers  = await Promise.all(topLosers.map(async l => { const u = await getUser(l._id); return { ...l, username: u.username, avatar: u.avatar }; }));
    const totalWins = await CasinoLog.countDocuments({ guildId: GUILD_ID, profit: { $gt: 0 } });
    const winRate   = statsAgg[0]?.totalBets ? Math.round((totalWins / statsAgg[0].totalBets) * 100) : 0;
    const activity  = {};
    for (let i = 6; i >= 0; i--) { const d = new Date(); d.setDate(d.getDate() - i); activity[d.toLocaleDateString('es-PE', { weekday: 'short', day: 'numeric' })] = { count: 0 }; }
    logs.forEach(l => { const k = new Date(l.timestamp).toLocaleDateString('es-PE', { weekday: 'short', day: 'numeric' }); if (activity[k]) activity[k].count++; });
    res.json({ logs: enrichedLogs, topWinners: enrichedWinners, topLosers: enrichedLosers, activity, stats: { ...statsAgg[0], winRate }, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/arms', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1, tab = req.query.tab || 'inventory', rarity = req.query.rarity || 'all', limit = 15;
    const query = { guildId: GUILD_ID }; if (rarity !== 'all') query.rarity = rarity;
    if (tab === 'inventory') {
      const [items, count] = await Promise.all([Arm.find(query).sort({ obtainedAt: -1 }).skip((page-1)*limit).limit(limit).lean(), Arm.countDocuments(query)]);
      const enriched = await Promise.all(items.map(async a => { const u = await getUser(a.userId); return { ...a, username: u.username, avatar: u.avatar }; }));
      return res.json({ items: enriched, pages: Math.ceil(count / limit) });
    } else if (tab === 'prices') {
      const [items, count] = await Promise.all([ArmCurrentPrice.find({ guildId: GUILD_ID }).sort({ currentPrice: -1 }).skip((page-1)*limit).limit(limit).lean(), ArmCurrentPrice.countDocuments({ guildId: GUILD_ID })]);
      return res.json({ items, pages: Math.ceil(count / limit) });
    } else if (tab === 'cases') {
      const recentOpens = await Arm.find(query).sort({ obtainedAt: -1 }).limit(limit).lean();
      const enriched = await Promise.all(recentOpens.map(async a => { const u = await getUser(a.userId); return { ...a, username: u.username, avatar: u.avatar }; }));
      return res.json({ recentOpens: enriched });
    } else if (tab === 'collectors') {
      const [collectors, distinct] = await Promise.all([
        Arm.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: '$userId', total: { $sum: 1 }, totalValue: { $sum: '$sellPrice' }, special: { $sum: { $cond: [{ $eq: ['$rarity','special'] }, 1, 0] } }, covert: { $sum: { $cond: [{ $eq: ['$rarity','covert'] }, 1, 0] } }, classified: { $sum: { $cond: [{ $eq: ['$rarity','classified'] }, 1, 0] } } } }, { $sort: { totalValue: -1 } }, { $skip: (page-1)*limit }, { $limit: limit }]),
        Arm.distinct('userId', { guildId: GUILD_ID }),
      ]);
      const enriched = await Promise.all(collectors.map(async (c, i) => { const u = await getUser(c._id); return { rank: (page-1)*limit+i+1, userId: c._id, username: u.username, avatar: u.avatar, ...c }; }));
      return res.json({ items: enriched, pages: Math.ceil(distinct.length / limit) });
    }
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/market', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1, tab = req.query.tab || 'listings', limit = 15;
    if (tab === 'listings') {
      const [items, count] = await Promise.all([MarketListing.find({ guildId: GUILD_ID, status: 'active' }).sort({ listedAt: -1 }).skip((page-1)*limit).limit(limit).lean(), MarketListing.countDocuments({ guildId: GUILD_ID, status: 'active' })]);
      const enriched = await Promise.all(items.map(async l => { const u = await getUser(l.sellerId); return { ...l, sellerName: u.username, sellerAvatar: u.avatar }; }));
      return res.json({ items: enriched, pages: Math.ceil(count / limit) });
    }
    if (tab === 'history') {
      const [txs, count] = await Promise.all([MarketTx.find({ guildId: GUILD_ID }).sort({ timestamp: -1 }).skip((page-1)*limit).limit(limit).lean(), MarketTx.countDocuments({ guildId: GUILD_ID })]);
      const enriched = await Promise.all(txs.map(async t => { const b = await getUser(t.buyerId), s = await getUser(t.sellerId); return { ...t, buyerName: b.username, sellerName: s.username }; }));
      return res.json({ items: enriched, pages: Math.ceil(count / limit) });
    }
    if (tab === 'chart') {
      const itemId = req.query.itemId;
      const topItems = await ArmCurrentPrice.find({ guildId: GUILD_ID }).sort({ currentPrice: -1 }).limit(10).lean();
      if (!itemId || !topItems.find(i => i.itemId === itemId)) return res.json({ topItems, history: [], current: null });
      const [history, current] = await Promise.all([ArmPrice.find({ guildId: GUILD_ID, itemId }).sort({ timestamp: -1 }).limit(100).lean(), ArmCurrentPrice.findOne({ guildId: GUILD_ID, itemId }).lean()]);
      return res.json({ topItems, history: history.reverse(), current });
    }
    if (tab === 'traders') {
      const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
      const [buyersAgg, sellersAgg, volAgg] = await Promise.all([
        MarketTx.aggregate([{ $match: { guildId: GUILD_ID, timestamp: { $gte: since } } }, { $group: { _id: '$buyerId', spent: { $sum: '$price' } } }, { $sort: { spent: -1 } }, { $limit: 10 }]),
        MarketTx.aggregate([{ $match: { guildId: GUILD_ID, timestamp: { $gte: since } } }, { $group: { _id: '$sellerId', earned: { $sum: '$price' } } }, { $sort: { earned: -1 } }, { $limit: 10 }]),
        MarketTx.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: null, total: { $sum: '$price' } } }]),
      ]);
      const topBuyers  = await Promise.all(buyersAgg.map(async b => { const u = await getUser(b._id); return { ...b, username: u.username, avatar: u.avatar }; }));
      const topSellers = await Promise.all(sellersAgg.map(async s => { const u = await getUser(s._id); return { ...s, username: u.username, avatar: u.avatar }; }));
      return res.json({ topBuyers, topSellers, totalVolume: volAgg[0]?.total || 0 });
    }
    res.json({ items: [], pages: 0 });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/config', requireAuth, async (req, res) => {
  try {
    const [channels, rewards, style] = await Promise.all([ChannelsConfig.findOne({ guildId: GUILD_ID }).lean(), RewardsConfig.findOne({ guildId: GUILD_ID }).lean(), GuildStyle.findOne({ guildId: GUILD_ID }).lean()]);
    res.json({ channels, rewards, style });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/achievements', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1, tab = req.query.tab || 'achievements', limit = 15;
    if (tab === 'achievements') {
      const [agg, count] = await Promise.all([Achievements.find({ guildId: GUILD_ID }).sort({ casino_wins: -1 }).skip((page-1)*limit).limit(limit).lean(), Achievements.countDocuments({ guildId: GUILD_ID })]);
      const enriched = await Promise.all(agg.map(async (a, i) => { const u = await getUser(a.userId); return { rank: (page-1)*limit+i+1, userId: a.userId, username: u.username, avatar: u.avatar, badges: a.badges?.length || 0, trivia_wins: a.trivia_wins || 0, casino_wins: a.casino_wins || 0, win_streak: a.win_streak || 0, max_bet: a.max_bet || 0 }; }));
      return res.json({ items: enriched, pages: Math.ceil(count / limit) });
    }
    if (tab === 'missions') {
      const now = new Date(), week = Math.ceil(((now - new Date(now.getFullYear(),0,1))/86400000 + new Date(now.getFullYear(),0,1).getDay() + 1) / 7);
      const [agg, count] = await Promise.all([MissionProgress.find({ guildId: GUILD_ID, year: now.getFullYear(), week }).sort({ claimed: 1, createdAt: -1 }).skip((page-1)*limit).limit(limit).lean(), MissionProgress.countDocuments({ guildId: GUILD_ID, year: now.getFullYear(), week })]);
      const enriched = await Promise.all(agg.map(async p => { const u = await getUser(p.userId); return { userId: p.userId, username: u.username, avatar: u.avatar, completed: p.missions?.filter(m => m.completed).length || 0, total: p.missions?.length || 0, claimed: p.claimed, missions: p.missions }; }));
      return res.json({ items: enriched, pages: Math.ceil(count / limit) });
    }
    if (tab === 'trivia') {
      const [agg, count] = await Promise.all([TriviaProgress.find({ guildId: GUILD_ID }).sort({ totalWins: -1 }).skip((page-1)*limit).limit(limit).lean(), TriviaProgress.countDocuments({ guildId: GUILD_ID })]);
      const enriched = await Promise.all(agg.map(async (t, i) => { const u = await getUser(t.userId); const tot = (t.totalWins||0)+(t.totalFails||0); return { rank: (page-1)*limit+i+1, userId: t.userId, username: u.username, avatar: u.avatar, totalWins: t.totalWins||0, totalFails: t.totalFails||0, streak: t.streak||0, winRate: tot>0?Math.round(t.totalWins/tot*100):0 }; }));
      return res.json({ items: enriched, pages: Math.ceil(count / limit) });
    }
    res.json({ items: [], pages: 0 });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/trades', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, status = req.query.status||'all', limit = 15;
    const query = { guildId: GUILD_ID }; if (status !== 'all') query.status = status;
    const [agg, count] = await Promise.all([Trade.find(query).sort({ createdAt: -1 }).skip((page-1)*limit).limit(limit).lean(), Trade.countDocuments(query)]);
    const enriched = await Promise.all(agg.map(async t => { const from = await getUser(t.fromUserId), to = await getUser(t.toUserId); return { ...t, fromUsername: from.username, fromAvatar: from.avatar, toUsername: to.username, toAvatar: to.avatar }; }));
    res.json({ trades: enriched, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/shop', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, active = req.query.active, limit = 20;
    const query = { guildId: GUILD_ID }; if (active === 'true') query.active = true; if (active === 'false') query.active = false;
    const [items, count] = await Promise.all([ShopItem.find(query).sort({ price: 1 }).skip((page-1)*limit).limit(limit).lean(), ShopItem.countDocuments(query)]);
    const enriched = await Promise.all(items.map(async item => { const u = await getUser(item.createdBy); return { ...item, createdByName: u.username }; }));
    res.json({ items: enriched, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/giveaway-blacklist', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, limit = 15;
    const [agg, count] = await Promise.all([GiveawayBlacklist.find({ guildId: GUILD_ID }).sort({ addedAt: -1 }).skip((page-1)*limit).limit(limit).lean(), GiveawayBlacklist.countDocuments({ guildId: GUILD_ID })]);
    const enriched = await Promise.all(agg.map(async b => { const u = await getUser(b.userId), a = await getUser(b.addedBy); return { ...b, username: u.username, avatar: u.avatar, addedByName: a.username }; }));
    res.json({ items: enriched, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/giveaway-config', requireAuth, async (req, res) => {
  try { const config = await GiveawayConfig.findOne({ guildId: GUILD_ID }).lean(); res.json({ config }); }
  catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/xpboosts', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, limit = 15, now = new Date();
    const [agg, count] = await Promise.all([XpBoost.find({ guildId: GUILD_ID, expiresAt: { $gt: now } }).sort({ expiresAt: 1 }).skip((page-1)*limit).limit(limit).lean(), XpBoost.countDocuments({ guildId: GUILD_ID, expiresAt: { $gt: now } })]);
    const enriched = await Promise.all(agg.map(async b => { const u = await getUser(b.userId), g = await getUser(b.grantedBy); return { ...b, username: u.username, avatar: u.avatar, grantedByName: g.username, hoursLeft: Math.max(0, Math.floor((new Date(b.expiresAt)-now)/3600000)) }; }));
    res.json({ items: enriched, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/games', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, game = req.query.game||'all', limit = 15;
    const query = { guildId: GUILD_ID }; if (game !== 'all') query.game = game;
    const [agg, count, topGames] = await Promise.all([
      GameHistory.find(query).sort({ endedAt: -1 }).skip((page-1)*limit).limit(limit).lean(),
      GameHistory.countDocuments(query),
      GameHistory.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: '$game', count: { $sum: 1 }, totalPot: { $sum: { $sum: '$players.amount' } } } }, { $sort: { count: -1 } }]),
    ]);
    const enriched = await Promise.all(agg.map(async g => {
      const players = await Promise.all((g.players||[]).slice(0,5).map(async p => { const u = await getUser(p.userId); return { ...p, username: u.username }; }));
      return { ...g, players };
    }));
    res.json({ games: enriched, pages: Math.ceil(count / limit), topGames });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/autoroles', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, limit = 15;
    const [agg, count] = await Promise.all([AutoRole.find({ guildId: GUILD_ID }).sort({ updatedAt: -1 }).skip((page-1)*limit).limit(limit).lean(), AutoRole.countDocuments({ guildId: GUILD_ID })]);
    res.json({ items: agg, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/backgrounds', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, filter = req.query.filter||'all', limit = 20;
    const query = { guildId: GUILD_ID }; if (filter === 'default') query.isDefault = true; if (filter === 'custom') query.isDefault = false;
    const [items, count] = await Promise.all([ServerBackground.find(query).sort({ createdAt: -1 }).skip((page-1)*limit).limit(limit).lean(), ServerBackground.countDocuments(query)]);
    const enriched = await Promise.all(items.map(async bg => { const u = await getUser(bg.createdBy); return { ...bg, createdByName: u.username }; }));
    res.json({ items: enriched, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  APIs DEL CASINO PÚBLICO
// ══════════════════════════════════════════════════════════════════════════
app.get('/api/casino/public/stats', async (req, res) => {
  try {
    const [totalBets, totalPlayers, volAgg, topWinners, topCoins] = await Promise.all([
      CasinoLog.countDocuments({ guildId: GUILD_ID }),
      UserCoins.countDocuments({ guildId: GUILD_ID }),
      CasinoLog.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: null, vol: { $sum: '$bet' } } }]),
      CasinoLog.aggregate([{ $match: { guildId: GUILD_ID, profit: { $gt: 0 } } }, { $group: { _id: '$userId', totalProfit: { $sum: '$profit' } } }, { $sort: { totalProfit: -1 } }, { $limit: 5 }]),
      UserCoins.find({ guildId: GUILD_ID }).sort({ coins: -1 }).limit(5).lean(),
    ]);
    const enrichW = await Promise.all(topWinners.map(async w => { const u = await getUser(w._id); return { ...w, username: u.username, avatar: u.avatar }; }));
    const enrichC = await Promise.all(topCoins.map(async c => { const u = await getUser(c.userId); return { ...c, username: u.username, avatar: u.avatar }; }));
    res.json({ totalBets, totalPlayers, totalVolume: volAgg[0]?.vol || 0, topWinners: enrichW, topCoins: enrichC });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/casino/public/market',    async (req, res) => { try { res.json({ items: await ArmCurrentPrice.find({ guildId: GUILD_ID }).sort({ currentPrice: -1 }).limit(30).lean() }); } catch (e) { res.status(500).json({ error: 'Error' }); } });
app.get('/api/casino/public/shop',      async (req, res) => { try { res.json({ items: await ShopItem.find({ guildId: GUILD_ID, active: true }).sort({ price: 1 }).lean() }); } catch (e) { res.status(500).json({ error: 'Error' }); } });

app.get('/api/casino/public/listings', async (req, res) => {
  try {
    const items = await MarketListing.find({ guildId: GUILD_ID, status: 'active' }).sort({ listedAt: -1 }).limit(20).lean();
    const enriched = await Promise.all(items.map(async l => { const u = await getUser(l.sellerId); return { ...l, sellerName: u.username }; }));
    res.json({ items: enriched });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/casino/public/leaderboard', async (req, res) => {
  try {
    const tab = req.query.tab || 'coins'; let items = [];
    if (tab === 'coins') {
      const agg = await UserCoins.find({ guildId: GUILD_ID }).sort({ coins: -1 }).limit(20).lean();
      items = await Promise.all(agg.map(async (u, i) => { const user = await getUser(u.userId); return { rank: i+1, username: user.username, avatar: user.avatar, coins: u.coins }; }));
    } else if (tab === 'level') {
      const agg = await Level.find({ guildId: GUILD_ID }).sort({ level: -1, xp: -1 }).limit(20).lean();
      items = await Promise.all(agg.map(async (l, i) => { const user = await getUser(l.userId); return { rank: i+1, username: user.username, avatar: user.avatar, level: l.level, xp: l.xp }; }));
    } else if (tab === 'casino') {
      const agg = await CasinoLog.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: '$userId', totalProfit: { $sum: '$profit' }, count: { $sum: 1 } } }, { $sort: { totalProfit: -1 } }, { $limit: 20 }]);
      items = await Promise.all(agg.map(async (u, i) => { const user = await getUser(u._id); return { rank: i+1, username: user.username, avatar: user.avatar, totalProfit: u.totalProfit }; }));
    } else if (tab === 'arms') {
      const agg = await Arm.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: '$userId', totalValue: { $sum: '$sellPrice' }, total: { $sum: 1 } } }, { $sort: { totalValue: -1 } }, { $limit: 20 }]);
      items = await Promise.all(agg.map(async (u, i) => { const user = await getUser(u._id); return { rank: i+1, username: user.username, avatar: user.avatar, totalValue: u.totalValue, total: u.total }; }));
    }
    res.json({ items });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/casino/me', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const coins = await UserCoins.findOne({ userId, guildId: GUILD_ID }).lean();
    if (!coins) return res.json({ registered: false, coins: 0 });
    const [level, ach] = await Promise.all([Level.findOne({ userId, guildId: GUILD_ID }).lean(), Achievements.findOne({ userId, guildId: GUILD_ID }).lean()]);
    res.json({ registered: true, coins: coins.coins, level: level?.level || 0, xp: level?.xp || 0, streak: level?.streak || 0, casinoWins: ach?.casino_wins || 0 });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.get('/api/casino/me/full', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const [coins, level, ach, arms] = await Promise.all([
      UserCoins.findOne({ userId, guildId: GUILD_ID }).lean(),
      Level.findOne({ userId, guildId: GUILD_ID }).lean(),
      Achievements.findOne({ userId, guildId: GUILD_ID }).lean(),
      Arm.find({ userId, guildId: GUILD_ID }).sort({ obtainedAt: -1 }).limit(30).lean(),
    ]);
    res.json({
      coins: coins?.coins || 0, totalEarned: coins?.totalEarned || 0,
      activeBgId: coins?.activeBgId || null, ownedBackgrounds: coins?.ownedBackgrounds || [],
      level: level?.level || 0, xp: level?.xp || 0, streak: level?.streak || 0,
      casinoWins: ach?.casino_wins || 0, registered: !!coins, arms: arms || [],
    });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  CASINO PLAY — juegos principales + NUEVOS (hilo, baccarat, plinko, videopoker)
//  Las monedas se modifican en UserCoins (mismo modelo que el bot)
//  El bot verá el cambio inmediatamente al consultar la DB
// ══════════════════════════════════════════════════════════════════════════

// Juegos manejados por handleExtendedPlay
const EXT_GAMES = ['carreras', 'loteria', 'minas', 'hilo', 'baccarat', 'plinko', 'videopoker'];

app.post('/api/casino/play', async (req, res) => {
  if (EXT_GAMES.includes(req.body?.game)) return handleExtendedPlay(req, res);
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const { game, bet, type, result } = req.body;
    if (!bet || bet < 1) return res.status(400).json({ error: 'Apuesta inválida' });
    const userCoins = await UserCoins.findOne({ userId, guildId: GUILD_ID });
    if (!userCoins) return res.status(403).json({ error: 'Usa /registrarse en Discord primero' });
    if (userCoins.coins < bet) return res.status(400).json({ error: 'Saldo insuficiente' });

    let won = false, winProfit = 0, number = null, slotsReels = null, multiplier = 1;

    if (game === 'ruleta') {
      number = Math.floor(Math.random() * 37);
      const reds   = new Set([1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]);
      const blacks = new Set([2,4,6,8,10,11,13,15,17,20,22,24,26,28,29,31,33,35]);
      const checks = { rojo: reds.has(number), negro: blacks.has(number), par: number !== 0 && number % 2 === 0, impar: number !== 0 && number % 2 !== 0, bajo: number >= 1 && number <= 18, alto: number >= 19 && number <= 36, docena1: number >= 1 && number <= 12, docena2: number >= 13 && number <= 24, docena3: number >= 25 && number <= 36, verde: number === 0 };
      const mults  = { rojo:2, negro:2, par:2, impar:2, bajo:2, alto:2, docena1:3, docena2:3, docena3:3, verde:35 };
      won = checks[type] || false; multiplier = mults[type] || 2;
      winProfit = won ? bet * (multiplier - 1) : 0;

    } else if (game === 'slots') {
      const syms = ['🍒','7️⃣','💰','🎰','⭐','🔔'];
      const s1 = Math.floor(Math.random() * 6), s2 = Math.floor(Math.random() * 6), s3 = Math.floor(Math.random() * 6);
      slotsReels = [syms[s1], syms[s2], syms[s3]];
      if (s1===s2 && s2===s3) { won=true; multiplier=10; }
      else if (s1===s2 || s2===s3 || s1===s3) { won=true; multiplier=2; }
      winProfit = won ? Math.floor(bet*(multiplier-1)) : 0;

    } else if (game === 'blackjack') {
      if (!['win','blackjack','push','lose','bust'].includes(result)) return res.status(400).json({ error: 'Resultado inválido' });
      won = ['win','blackjack','push'].includes(result);
      winProfit = result==='blackjack' ? Math.floor(bet*1.5) : result==='win' ? bet : 0;
      multiplier = result==='blackjack' ? 2.5 : 2;
    }

    const delta   = won ? winProfit : -bet;
    const updated = await applyCoins(userId, delta, won);
    await logAndAchieve(userId, game, bet, won, delta);
    res.json({ won, number, multiplier, reels: slotsReels, newBalance: updated.coins, profit: delta });
  } catch (e) { console.error('[casino/play]', e); res.status(500).json({ error: 'Error' }); }
});

// ── CRASH ─────────────────────────────────────────────────────────────────
app.post('/api/casino/crash/start', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id, { bet } = req.body;
    if (!bet || bet < 10) return res.status(400).json({ error: 'Mínimo 10' });
    const userCoins = await UserCoins.findOne({ userId, guildId: GUILD_ID });
    if (!userCoins) return res.status(403).json({ error: 'Usa /registrarse en Discord primero' });
    if (userCoins.coins < bet) return res.status(400).json({ error: 'Saldo insuficiente' });
    const r = Math.random();
    let crashAt;
    if      (r < 0.50) crashAt = 1 + Math.random() * 0.8;
    else if (r < 0.75) crashAt = 1.8 + Math.random() * 1.2;
    else if (r < 0.90) crashAt = 3 + Math.random() * 4;
    else if (r < 0.97) crashAt = 7 + Math.random() * 8;
    else               crashAt = 15 + Math.random() * 5;
    crashAt = Math.round(crashAt * 100) / 100;
    req.session.crashGame = { bet, crashAt, userId, ts: Date.now() };
    res.json({ crashAt });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.post('/api/casino/crash/cashout', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id, { bet, mult } = req.body, game = req.session.crashGame;
    if (!game || game.userId !== userId || game.bet !== bet) return res.status(400).json({ error: 'Juego inválido' });
    if (mult > game.crashAt + 0.05) return res.status(400).json({ error: 'Multiplicador inválido' });
    if (Date.now() - game.ts > 120000) return res.status(400).json({ error: 'Tiempo expirado' });
    req.session.crashGame = null;
    const winProfit = Math.floor(bet * mult) - bet;
    const updated   = await applyCoins(userId, winProfit, true);
    await logAndAchieve(userId, 'crash', bet, true, winProfit);
    res.json({ won: true, newBalance: updated.coins, profit: winProfit });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ── SHOP BUY ──────────────────────────────────────────────────────────────
app.post('/api/casino/buy', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id, { itemId } = req.body;
    const item = await ShopItem.findOne({ guildId: GUILD_ID, itemId, active: true }).lean();
    if (!item) return res.status(404).json({ error: 'Item no encontrado' });
    const userCoins = await UserCoins.findOne({ userId, guildId: GUILD_ID });
    if (!userCoins || userCoins.coins < item.price) return res.status(400).json({ error: 'Saldo insuficiente' });
    const updated = await applyCoins(userId, -item.price, false);
    await ShopPurchase.create({ userId, guildId: GUILD_ID, itemId, itemName: item.name, price: item.price }).catch(() => {});
    res.json({ success: true, newBalance: updated.coins });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  handleExtendedPlay — carreras, lotería, minas + NUEVOS JUEGOS
//  Todos modifican UserCoins → el bot de Discord los verá en tiempo real
// ══════════════════════════════════════════════════════════════════════════
const HORSES_DATA = { A:{baseSpeed:.35,variance:.40}, B:{baseSpeed:.38,variance:.30}, C:{baseSpeed:.32,variance:.50}, D:{baseSpeed:.40,variance:.20}, E:{baseSpeed:.30,variance:.60} };

// Helpers para juegos del servidor
function bacCV(rank){ if(['J','Q','K','10'].includes(rank)) return 0; if(rank==='A') return 1; return parseInt(rank); }
function bacTotal(hand){ return hand.reduce((s,c)=>s+bacCV(c.rank),0)%10; }
function bacSimulate(){
  const RANKS=['2','3','4','5','6','7','8','9','10','J','Q','K','A'];
  const SUITS=['♠','♥','♦','♣'];
  const deck=RANKS.flatMap(r=>SUITS.map(s=>({rank:r,suit:s}))).sort(()=>Math.random()-.5);
  let pi=0; const deal=()=>deck[pi++];
  let pH=[deal(),deal()], bH=[deal(),deal()];
  const pT=bacTotal(pH), bT=bacTotal(bH);
  let pD=false, bD=false;
  if(pT<=5) pD=true;
  if(pD){ const c3=deal(); pH.push(c3); const pv3=bacCV(c3.rank); if(bT<=2)bD=true; else if(bT===3&&pv3!==8)bD=true; else if(bT===4&&[2,3,4,5,6,7].includes(pv3))bD=true; else if(bT===5&&[4,5,6,7].includes(pv3))bD=true; else if(bT===6&&[6,7].includes(pv3))bD=true; }
  else{ if(bT<=5)bD=true; }
  if(bD) bH.push(deal());
  const pF=bacTotal(pH), bF=bacTotal(bH);
  return pF>bF?'player':bF>pF?'banker':'tie';
}

const PLINKO_MULTS_SRV = { low:[1.5,1.2,1.1,1.0,0.5,1.0,1.1,1.2,1.5], medium:[3.0,1.5,1.2,0.8,0.3,0.8,1.2,1.5,3.0], high:[100,9,4,2,0.2,2,4,9,100] };
function plinkoSimulate(risk){
  let col=0; for(let i=0;i<8;i++) col+=Math.random()<.5?1:0; col=Math.min(col,8);
  return{ col, mult:(PLINKO_MULTS_SRV[risk]||PLINKO_MULTS_SRV.low)[col] };
}

async function handleExtendedPlay(req, res) {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const { game, bet, horse, winner, result, profit: clientProfit, myBet, risk } = req.body;
    if (!bet || bet < 1) return res.status(400).json({ error: 'Apuesta inválida' });
    const userCoins = await UserCoins.findOne({ userId, guildId: GUILD_ID });
    if (!userCoins) return res.status(403).json({ error: 'Usa /registrarse en Discord primero' });
    if (userCoins.coins < bet) return res.status(400).json({ error: 'Saldo insuficiente' });

    let won = false, delta = -bet;

    // ── Juegos originales ──────────────────────────────────────────────────
    if (game === 'carreras') {
      const pos = Object.fromEntries(Object.keys(HORSES_DATA).map(k => [k, 0]));
      let serverWinner = null;
      for (let t = 0; t < 200 && !serverWinner; t++) {
        for (const [id, h] of Object.entries(HORSES_DATA)) {
          let adv = 0;
          if (Math.random() < h.baseSpeed + (Math.random()-.5)*h.variance) adv = 1;
          if (Math.random() < .05) adv++;
          pos[id] = Math.min(20, pos[id] + adv);
        }
        const fin = Object.entries(pos).filter(([,v]) => v >= 20);
        if (fin.length) serverWinner = fin[0][0];
      }
      if (!serverWinner) serverWinner = winner;
      won = serverWinner === horse;
      delta = won ? bet : -bet;

    } else if (game === 'loteria') {
      const LOT = ['🍒','⭐','💎','🍀','🔔','🎯','🎰','💰','🌙'];
      const grid = Array.from({length:9}, () => LOT[Math.floor(Math.random()*LOT.length)]);
      const counts = {}; for (const s of grid) counts[s] = (counts[s]||0)+1;
      const max = Math.max(...Object.values(counts));
      const mult = max>=7?3.0:max>=5?2.0:max>=4?1.5:max>=3?1.0:0;
      won = mult > 0;
      delta = won ? Math.floor(bet * mult) - bet : -bet;

    } else if (game === 'minas') {
      if (result === 'cashout' && clientProfit > 0) {
        won = true;
        delta = Math.min(clientProfit, bet * 20); // techo anti-cheat: máx 20×
      } else {
        won = false; delta = -bet;
      }

    // ── NUEVOS JUEGOS ──────────────────────────────────────────────────────
    // Hi-Lo: el cliente controla la lógica de cartas (son aleatorias en JS)
    // El servidor valida que el profit no exceda un límite razonable
    } else if (game === 'hilo') {
      if (result === 'cashout' && clientProfit > 0) {
        won = true;
        delta = Math.min(clientProfit, bet * 500); // techo: máx 500× (racha de ~9)
      } else {
        won = false; delta = -bet;
      }

    // Baccarat: el servidor re-simula las cartas independientemente
    // Garantiza que el resultado no se puede falsificar desde el cliente
    } else if (game === 'baccarat') {
      const serverOutcome = bacSimulate();
      const betTarget = myBet || 'player';
      won = serverOutcome === betTarget;
      const multMap = { player: 2, banker: 1.95, tie: 9 };
      const m = multMap[betTarget] || 2;
      delta = won ? Math.floor(bet * (m - 1)) : -bet;

    // Plinko: el servidor re-simula el camino de la bola
    // El cliente solo ve la animación; el resultado real viene del servidor
    } else if (game === 'plinko') {
      const riskLevel = risk || 'low';
      const { mult: serverMult } = plinkoSimulate(riskLevel);
      const serverProfit = Math.floor(bet * serverMult) - bet;
      won = serverProfit > 0;
      delta = serverProfit;

    // Video Poker: el servidor valida el profit contra el máximo posible (800×)
    } else if (game === 'videopoker') {
      if (result === 'win' && clientProfit > 0) {
        won = true;
        delta = Math.min(clientProfit, bet * 800); // techo: Royal Flush 800×
      } else {
        won = false; delta = -bet;
      }

    } else {
      return res.status(400).json({ error: 'Juego no reconocido' });
    }

    // Aplicar cambio de monedas en DB (compartida con el bot)
    const updated = await applyCoins(userId, delta, won);
    await logAndAchieve(userId, game, bet, won, delta);
    res.json({ won, profit: delta, newBalance: updated.coins });
  } catch (e) { console.error('[casino/extended]', e); res.status(500).json({ error: 'Error' }); }
}

// ── BATALLA ───────────────────────────────────────────────────────────────
app.post('/api/casino/battle', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const { bet, opponentId } = req.body;
    if (!bet || bet < 100) return res.status(400).json({ error: 'Mínimo 100' });
    if (!opponentId || opponentId === userId) return res.status(400).json({ error: 'Rival inválido' });
    const [myCoins, opCoins] = await Promise.all([UserCoins.findOne({ userId, guildId: GUILD_ID }), UserCoins.findOne({ userId: opponentId, guildId: GUILD_ID })]);
    if (!myCoins || myCoins.coins < bet) return res.status(400).json({ error: 'Saldo insuficiente' });
    if (!opCoins || opCoins.coins < bet) return res.status(400).json({ error: 'El rival no tiene suficientes monedas' });
    const myRoll = Math.floor(Math.random()*6)+1, opRoll = Math.floor(Math.random()*6)+1;
    const tie = myRoll === opRoll, iWon = myRoll > opRoll;
    if (!tie) {
      const winner = iWon ? myCoins : opCoins, loser = iWon ? opCoins : myCoins;
      winner.coins += bet; winner.totalEarned += bet; loser.coins -= bet;
      await Promise.all([winner.save(), loser.save()]);
      await Promise.all([
        CasinoLog.create({ userId, guildId: GUILD_ID, game: 'batalla', bet, result: iWon?'win':'loss', profit: iWon?bet:-bet }),
        CasinoLog.create({ userId: opponentId, guildId: GUILD_ID, game: 'batalla', bet, result: iWon?'loss':'win', profit: iWon?-bet:bet }),
      ]);
      if (iWon) await Achievements.findOneAndUpdate({ userId, guildId: GUILD_ID }, { $inc: { casino_wins: 1 }, $max: { max_bet: bet } }, { upsert: true });
      if (global.io) {
        global.io.emit('coins:update', { userId, coins: myCoins.coins });
        global.io.emit('coins:update', { userId: opponentId, coins: opCoins.coins });
        const u = await getUser(userId);
        global.io.emit('casino:bet', { userId, username: u.username, game: 'batalla', bet, profit: iWon?bet:-bet });
      }
    }
    res.json({ won: iWon, tie, myRoll, opponentRoll: opRoll, newBalance: myCoins.coins });
  } catch (e) { console.error('[battle]', e); res.status(500).json({ error: 'Error' }); }
});

// ── TRIVIA ────────────────────────────────────────────────────────────────
const TRIVIA_POOL = [
  {q:'¿Cuántos continentes hay?',o:['5','6','7','8'],c:2,d:'easy'},
  {q:'Capital de Francia',o:['Lyon','París','Roma','Berlín'],c:1,d:'easy'},
  {q:'Planeta más grande del sistema solar',o:['Saturno','Neptuno','Júpiter','Urano'],c:2,d:'medium'},
  {q:'¿Cuántos huesos tiene el cuerpo humano adulto?',o:['186','206','226','246'],c:1,d:'medium'},
  {q:'¿Quién pintó La Mona Lisa?',o:['Van Gogh','Picasso','Da Vinci','Monet'],c:2,d:'hard'},
  {q:'¿En qué año llegó el hombre a la Luna?',o:['1965','1967','1969','1971'],c:2,d:'medium'},
  {q:'¿Cuál es el elemento más abundante en el universo?',o:['Oxígeno','Helio','Hidrógeno','Carbono'],c:2,d:'hard'},
  {q:'¿Cuántos bytes tiene un kilobyte?',o:['100','512','1000','1024'],c:3,d:'medium'},
  {q:'¿Quién escribió Don Quijote?',o:['Cervantes','Lope de Vega','Quevedo','Góngora'],c:0,d:'hard'},
  {q:'¿Cuántos colores tiene el arcoíris?',o:['5','6','7','8'],c:2,d:'easy'},
  {q:'Capital de Japón',o:['Osaka','Kioto','Tokio','Hiroshima'],c:2,d:'easy'},
  {q:'Fórmula química del agua',o:['H2O','CO2','O2','H2O2'],c:0,d:'medium'},
  {q:'¿Quién formuló la relatividad?',o:['Newton','Tesla','Einstein','Hawking'],c:2,d:'hard'},
  {q:'¿Cuántos días tiene febrero en año normal?',o:['28','29','30','31'],c:0,d:'easy'},
  {q:'Capital de Perú',o:['Cusco','Lima','Arequipa','Trujillo'],c:1,d:'easy'},
  {q:'¿Cuántos lados tiene un hexágono?',o:['4','5','6','8'],c:2,d:'easy'},
  {q:'¿En qué año se fundó Google?',o:['1996','1998','2000','2002'],c:1,d:'medium'},
  {q:'Metal más abundante en la corteza terrestre',o:['Hierro','Aluminio','Cobre','Oro'],c:1,d:'hard'},
  {q:'¿Quién escribió 1984?',o:['Orwell','Huxley','Bradbury','Wells'],c:0,d:'hard'},
  {q:'Capital de Argentina',o:['Córdoba','Rosario','Buenos Aires','Mendoza'],c:2,d:'easy'},
];
const TRIVIA_REWARDS = { easy: 10, medium: 20, hard: 35 };

app.get('/api/casino/trivia/questions', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const coins = await UserCoins.findOne({ userId, guildId: GUILD_ID }).lean();
    if (!coins) return res.status(403).json({ error: 'Usa /registrarse primero' });
    const pick = (d, n) => TRIVIA_POOL.filter(q => q.d === d).sort(() => Math.random()-.5).slice(0, n);
    const questions = [...pick('easy',2), ...pick('medium',2), ...pick('hard',2)].sort(() => Math.random()-.5).map(q => ({ q: q.q, o: q.o, d: q.d }));
    req.session.triviaSession = {
      questions: questions.map(qq => { const orig = TRIVIA_POOL.find(p => p.q === qq.q); return { ...qq, c: orig?.c ?? 0 }; }),
      answered: [], ts: Date.now(),
    };
    res.json({ questions });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.post('/api/casino/trivia/answer', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id, { idx, answer } = req.body, ts = req.session.triviaSession;
    if (!ts || Date.now()-ts.ts > 600000) return res.status(400).json({ error: 'Sesión expirada' });
    if (ts.answered.includes(idx)) return res.status(400).json({ error: 'Ya respondida' });
    ts.answered.push(idx);
    const q = ts.questions[idx], correct = answer === q.c, reward = correct ? TRIVIA_REWARDS[q.d] || 10 : 0;
    let newBalance = 0;
    if (correct && reward > 0) {
      const updated = await applyCoins(userId, reward, true);
      newBalance = updated?.coins || 0;
    } else {
      const uc = await UserCoins.findOne({ userId, guildId: GUILD_ID }).lean();
      newBalance = uc?.coins || 0;
    }
    await TriviaProgress.findOneAndUpdate({ userId, guildId: GUILD_ID }, { $inc: { totalWins: correct?1:0, totalFails: correct?0:1 } }, { upsert: true });
    res.json({ correct, correctAnswer: q.c, reward, newBalance });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ── CAJAS ─────────────────────────────────────────────────────────────────
const CASES_CONFIG = {
  iniciacion:  { price: 800,   rarities: ['consumer','industrial','milspec'],   weights: [7992,1598,320], specialChance: 0 },
  clasificada: { price: 2000,  rarities: ['industrial','milspec','restricted'], weights: [1598,320,64],   specialChance: 0 },
  elite:       { price: 5000,  rarities: ['milspec','restricted','classified'], weights: [320,64,13],     specialChance: 1 },
  dorada:      { price: 12000, rarities: ['restricted','classified','covert'],  weights: [64,13,3],       specialChance: 3 },
  contrabando: { price: 25000, rarities: ['classified','covert','special'],     weights: [13,3,1],        specialChance: 8 },
};
const RARITY_ITEMS = {
  consumer:   [{id:'p250_sand_dune',name:'P250',skin:'Sand Dune',bp:500},{id:'glock_sand_dune',name:'Glock-18',skin:'Sand Dune',bp:450},{id:'famas_doomkitty',name:'FAMAS',skin:'Doomkitty',bp:800},{id:'aug_storm',name:'AUG',skin:'Storm',bp:680},{id:'m249_system_lock',name:'M249',skin:'System Lock',bp:490}],
  industrial: [{id:'ak47_blue_laminate',name:'AK-47',skin:'Blue Laminate',bp:2200},{id:'ak47_jungle_spray',name:'AK-47',skin:'Jungle Spray',bp:2500},{id:'awp_safari_mesh',name:'AWP',skin:'Safari Mesh',bp:3800},{id:'deagle_midnight_storm',name:'Desert Eagle',skin:'Midnight Storm',bp:2400},{id:'bizon_osiris',name:'PP-Bizon',skin:'Osiris',bp:1600}],
  milspec:    [{id:'ak47_neon_rider',name:'AK-47',skin:'Neon Rider',bp:10000},{id:'deagle_printstream',name:'Desert Eagle',skin:'Printstream',bp:13000},{id:'awp_chromatic',name:'AWP',skin:'Chromatic Aberration',bp:14000},{id:'m4a1_flashback',name:'M4A1-S',skin:'Flashback',bp:12000},{id:'five7_hyper_beast',name:'Five-SeveN',skin:'Hyper Beast',bp:9500}],
  restricted: [{id:'ak47_wasteland',name:'AK-47',skin:'Wasteland Rebel',bp:35000},{id:'awp_pit_viper',name:'AWP',skin:'Pit Viper',bp:50000},{id:'m4a1_nightmare',name:'M4A1-S',skin:'Nightmare',bp:45000},{id:'deagle_golden_koi',name:'Desert Eagle',skin:'Golden Koi',bp:32000},{id:'usp_neo_noir',name:'USP-S',skin:'Neo-Noir',bp:38000}],
  classified: [{id:'ak47_wild_lotus',name:'AK-47',skin:'Wild Lotus',bp:150000},{id:'m4a4_howl',name:'M4A4',skin:'Howl',bp:180000},{id:'awp_medusa',name:'AWP',skin:'Medusa',bp:170000},{id:'deagle_blaze',name:'Desert Eagle',skin:'Blaze',bp:130000},{id:'usp_kill_confirmed',name:'USP-S',skin:'Kill Confirmed',bp:140000}],
  covert:     [{id:'ak47_fire_serpent',name:'AK-47',skin:'Fire Serpent',bp:550000},{id:'awp_dragon_lore',name:'AWP',skin:'Dragon Lore',bp:600000},{id:'awp_gungnir',name:'AWP',skin:'Gungnir',bp:520000},{id:'m4a4_poseidon',name:'M4A4',skin:'Poseidon',bp:420000},{id:'ak47_case_hardened',name:'AK-47',skin:'Case Hardened',bp:350000}],
  special:    [{id:'knife_karambit_doppler',name:'Karambit',skin:'Doppler Fase 2',bp:1200000},{id:'knife_butterfly_tiger',name:'Butterfly Knife',skin:'Tiger Tooth',bp:1000000},{id:'knife_m9_fade',name:'M9 Bayoneta',skin:'Fade',bp:950000},{id:'gloves_sport_pandeo',name:'Sport Gloves',skin:'Pandeo',bp:1500000},{id:'knife_bayonet_slaughter',name:'Bayoneta',skin:'Slaughter',bp:800000}],
};
const WEARS_DATA = [{name:'Factory New',tag:'FN',mult:1.0},{name:'Minimal Wear',tag:'MW',mult:.80},{name:'Field-Tested',tag:'FT',mult:.60},{name:'Well-Worn',tag:'WW',mult:.40},{name:'Battle-Scarred',tag:'BS',mult:.20}];
const WEAR_W     = [10,24,33,24,9];
const RARITY_META= {consumer:{color:0x9ea3b0,sellMult:.55},industrial:{color:0x5e98d9,sellMult:.55},milspec:{color:0x4b69ff,sellMult:.55},restricted:{color:0x8847ff,sellMult:.60},classified:{color:0xd32ce6,sellMult:.60},covert:{color:0xeb4b4b,sellMult:.65},special:{color:0xffd700,sellMult:.70}};

function rollCase(caseId) {
  const box = CASES_CONFIG[caseId]; if (!box) return null;
  let rarity;
  const sr = Math.random()*100;
  if (sr < box.specialChance) { rarity = 'special'; }
  else {
    const tw = box.weights.reduce((a,b)=>a+b,0); let r = Math.random()*tw;
    rarity = box.rarities[box.rarities.length-1];
    for (let i = 0; i < box.rarities.length; i++) { r -= box.weights[i]; if (r <= 0) { rarity = box.rarities[i]; break; } }
  }
  const pool = RARITY_ITEMS[rarity], item = pool[Math.floor(Math.random()*pool.length)];
  const tw2 = WEAR_W.reduce((a,b)=>a+b,0); let wr = Math.random()*tw2, wi = WEAR_W.length-1;
  for (let i = 0; i < WEAR_W.length; i++) { wr -= WEAR_W[i]; if (wr <= 0) { wi = i; break; } }
  const wear = WEARS_DATA[wi], rm = RARITY_META[rarity];
  return { itemId: item.id, name: item.name, skin: item.skin, type: 'rifle', emoji: '🔫', rarity, rarityName: rarity, rarityColor: rm.color, wear: wear.name, wearTag: wear.tag, basePrice: item.bp, sellPrice: Math.floor(item.bp*wear.mult*rm.sellMult), caseId };
}

app.post('/api/casino/case/open', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id, { caseId } = req.body;
    const box = CASES_CONFIG[caseId];
    if (!box) return res.status(400).json({ error: 'Caja inválida' });
    const uc = await UserCoins.findOne({ userId, guildId: GUILD_ID });
    if (!uc) return res.status(403).json({ error: 'Usa /registrarse primero' });
    if (uc.coins < box.price) return res.status(400).json({ error: 'Saldo insuficiente' });
    const drop = rollCase(caseId);
    if (!drop) return res.status(500).json({ error: 'Error generando drop' });
    await Promise.all([
      applyCoins(userId, -box.price, false),
      Arm.create({ userId, guildId: GUILD_ID, ...drop, obtainedAt: new Date() }),
    ]);
    const updated = await UserCoins.findOne({ userId, guildId: GUILD_ID }).lean();
    res.json({ drop, newBalance: updated.coins });
  } catch (e) { console.error('[case]', e); res.status(500).json({ error: 'Error' }); }
});

app.get('/api/casino/inventory', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const arms = await Arm.find({ userId: req.session.user.id, guildId: GUILD_ID }).sort({ obtainedAt: -1 }).lean();
    res.json({ arms });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.post('/api/casino/market/buy', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id, { listingId } = req.body;
    const listing = await MarketListing.findOne({ _id: listingId, guildId: GUILD_ID, status: 'active' });
    if (!listing) return res.status(404).json({ error: 'Listing no encontrado o ya vendido' });
    if (listing.sellerId === userId) return res.status(400).json({ error: 'No puedes comprarte a ti mismo' });
    const uc = await UserCoins.findOne({ userId, guildId: GUILD_ID });
    if (!uc || uc.coins < listing.price) return res.status(400).json({ error: 'Saldo insuficiente' });
    await Promise.all([
      applyCoins(userId, -listing.price, false),
      UserCoins.findOneAndUpdate({ userId: listing.sellerId, guildId: GUILD_ID }, { $inc: { coins: listing.price, totalEarned: listing.price } }),
      MarketListing.findByIdAndUpdate(listingId, { status: 'sold', buyerId: userId, soldAt: new Date() }),
      Arm.findOneAndUpdate({ _id: listing.armId }, { userId, tradeId: null }),
      MarketTx.create({ guildId: GUILD_ID, itemId: listing.itemId, name: listing.name, skin: listing.skin, rarity: listing.rarity, price: listing.price, sellerId: listing.sellerId, buyerId: userId }),
    ]);
    const updated = await UserCoins.findOne({ userId, guildId: GUILD_ID }).lean();
    if (global.io) global.io.emit('market:update', {});
    res.json({ success: true, newBalance: updated.coins });
  } catch (e) { console.error('[market/buy]', e); res.status(500).json({ error: 'Error' }); }
});

app.get('/api/admin/purchases', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, limit = 15;
    const [agg, count] = await Promise.all([
      ShopPurchase.find({ guildId: GUILD_ID }).sort({ purchasedAt: -1 }).skip((page-1)*limit).limit(limit).lean(),
      ShopPurchase.countDocuments({ guildId: GUILD_ID }),
    ]);
    const enriched = await Promise.all(agg.map(async p => { const u = await getUser(p.userId); return { ...p, username: u.username, avatar: u.avatar }; }));
    res.json({ items: enriched, pages: Math.ceil(count / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  NUEVOS SCHEMAS
// ══════════════════════════════════════════════════════════════════════════
const BankAccount = mongoose.models.BankAccount || model('BankAccount', new Schema({
  userId: String, guildId: String, balance: { type: Number, default: 0 },
  lastInterest: { type: Date, default: null },
  history: [{ type: { type: String }, amount: Number, note: String, timestamp: { type: Date, default: Date.now } }],
}));
const ChatMessage = mongoose.models.ChatMessage || model('ChatMessage', new Schema({
  userId: String, username: String, avatar: String, guildId: String, message: String,
  timestamp: { type: Date, default: Date.now },
}));
const WeeklyTournament = mongoose.models.WeeklyTournament || model('WeeklyTournament', new Schema({
  guildId: String, week: Number, year: Number,
  prizes: [{ place: Number, userId: String, amount: Number, claimed: Boolean }],
  ended: { type: Boolean, default: false }, endedAt: Date,
  createdAt: { type: Date, default: Date.now },
}));
const TradeOffer = mongoose.models.TradeOffer || model('TradeOffer', new Schema({
  guildId: String, fromUserId: String, toUserId: String,
  fromArmId: mongoose.Schema.Types.ObjectId, toArmId: mongoose.Schema.Types.ObjectId,
  fromArmName: String, toArmName: String,
  status: { type: String, default: 'pending' }, message: String,
  expiresAt: Date, createdAt: { type: Date, default: Date.now },
}));
const DailyMission = mongoose.models.DailyMission || model('DailyMission', new Schema({
  userId: String, guildId: String, date: String,
  missions: [{ id: String, name: String, desc: String, target: Number, progress: { type: Number, default: 0 }, reward: Number, completed: { type: Boolean, default: false }, claimed: { type: Boolean, default: false } }],
  createdAt: { type: Date, default: Date.now },
}));
const CasinoBan = mongoose.models.CasinoBan || model('CasinoBan', new Schema({
  userId: String, guildId: String, reason: String, bannedBy: String,
  expiresAt: Date, createdAt: { type: Date, default: Date.now },
}));
const FriendRequest = mongoose.models.FriendRequest || model('FriendRequest', new Schema({
  guildId: String, fromUserId: String, toUserId: String,
  status: { type: String, default: 'pending' }, createdAt: { type: Date, default: Date.now },
}));

// ── Rate limiting ─────────────────────────────────────────────────────────
const betRateMap = new Map();
function checkRateLimit(userId) {
  const now = Date.now(), window = 60000, max = 15;
  if (!betRateMap.has(userId)) betRateMap.set(userId, []);
  const times = betRateMap.get(userId).filter(t => now - t < window);
  if (times.length >= max) return false;
  times.push(now); betRateMap.set(userId, times); return true;
}
// ── Ban check ─────────────────────────────────────────────────────────────
async function checkCasinoBan(userId) {
  return CasinoBan.findOne({ userId, guildId: GUILD_ID, $or: [{ expiresAt: null }, { expiresAt: { $gt: new Date() } }] }).lean();
}
// ── Límite pérdida diaria ─────────────────────────────────────────────────
const dailyLossMap = new Map();
const MAX_DAILY_LOSS = 100000;
function trackDailyLoss(userId, loss) {
  const today = new Date().toDateString();
  const e = dailyLossMap.get(userId);
  if (!e || e.date !== today) { dailyLossMap.set(userId, { loss: Math.max(0, loss), date: today }); return true; }
  e.loss += Math.max(0, loss);
  return e.loss <= MAX_DAILY_LOSS;
}
// ── Alerta ganancias sospechosas ──────────────────────────────────────────
async function checkSuspiciousWin(userId, profit, game) {
  if (profit >= 50000) {
    const u = await getUser(userId);
    console.warn(`[ALERTA] ${u.username} ganó ${profit} en ${game}`);
    if (global.io) global.io.emit('casino:alert', { type: 'big_win', userId, username: u.username, profit, game, timestamp: new Date() });
  }
}
// ── Actualizar misiones ───────────────────────────────────────────────────
async function updateMissionProgress(userId, game, won, betAmount) {
  try {
    const today = new Date().toISOString().split('T')[0];
    const daily = await DailyMission.findOne({ userId, guildId: GUILD_ID, date: today });
    if (!daily) return;
    let changed = false;
    daily.missions.forEach(m => {
      if (m.completed || m.claimed) return;
      if (m.id === 'play5' || m.id === 'play10') { m.progress++; changed = true; }
      if ((m.id === 'win3' || m.id === 'win5') && won) { m.progress++; changed = true; }
      if ((m.id === 'bet1000' || m.id === 'bet5000') && betAmount > 0) { m.progress += betAmount; changed = true; }
      if (m.id === 'slots3' && game === 'slots') { m.progress++; changed = true; }
      if (m.id === 'trivia2' && game === 'trivia' && won) { m.progress++; changed = true; }
      if (m.progress >= m.target) m.completed = true;
    });
    if (changed) await daily.save();
  } catch {}
}

// ══════════════════════════════════════════════════════════════════════════
//  HISTORIAL PERSONAL
// ══════════════════════════════════════════════════════════════════════════
app.get('/api/casino/history', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const { id: userId } = req.session.user;
    const filter = req.query.filter || 'all';
    const query = { userId, guildId: GUILD_ID };
    if (filter === 'win') query.result = 'win';
    if (filter === 'loss') query.result = 'loss';
    const logs = await CasinoLog.find(query).sort({ timestamp: -1 }).limit(50).lean();
    res.json({ logs });
  } catch { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  ESTADÍSTICAS PERSONALES
// ══════════════════════════════════════════════════════════════════════════
app.get('/api/casino/my-stats', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const [logs, ach] = await Promise.all([
      CasinoLog.find({ userId, guildId: GUILD_ID }).sort({ timestamp: -1 }).limit(500).lean(),
      Achievements.findOne({ userId, guildId: GUILD_ID }).lean(),
    ]);
    const totalWins = logs.filter(l => l.profit > 0).length;
    const totalBet  = logs.reduce((s, l) => s + (l.bet || 0), 0);
    const netProfit = logs.reduce((s, l) => s + (l.profit || 0), 0);
    const bestWin   = Math.max(0, ...logs.map(l => l.profit || 0));
    const maxBet    = Math.max(0, ...logs.map(l => l.bet || 0));
    const byGame = {};
    logs.forEach(l => {
      if (!byGame[l.game]) byGame[l.game] = { plays: 0, wins: 0, net: 0 };
      byGame[l.game].plays++;
      if (l.profit > 0) byGame[l.game].wins++;
      byGame[l.game].net += l.profit || 0;
    });
    let currentStreak = 0;
    for (const l of logs.slice(0, 20)) { if (l.profit > 0) currentStreak++; else break; }
    res.json({ totalWins, totalBet, netProfit, bestWin, maxBet, byGame, currentStreak, bestStreak: ach?.win_streak || 0 });
  } catch { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  BANCO
// ══════════════════════════════════════════════════════════════════════════
app.get('/api/casino/bank/info', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const bank = await BankAccount.findOne({ userId: req.session.user.id, guildId: GUILD_ID }).lean();
    res.json({ balance: bank?.balance || 0, lastInterest: bank?.lastInterest || null });
  } catch { res.status(500).json({ error: 'Error' }); }
});
app.get('/api/casino/bank/history', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const bank = await BankAccount.findOne({ userId: req.session.user.id, guildId: GUILD_ID }).lean();
    res.json({ history: (bank?.history || []).slice(-20).reverse() });
  } catch { res.status(500).json({ error: 'Error' }); }
});
app.post('/api/casino/bank', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const { action, amount } = req.body;
    if (!amount || amount < 1) return res.status(400).json({ error: 'Cantidad inválida' });
    const wallet = await UserCoins.findOne({ userId, guildId: GUILD_ID });
    if (!wallet) return res.status(403).json({ error: 'Usa /registrarse primero' });
    let bank = await BankAccount.findOne({ userId, guildId: GUILD_ID });
    if (!bank) bank = new BankAccount({ userId, guildId: GUILD_ID, balance: 0, history: [] });
    if (bank.lastInterest) {
      const hrs = (Date.now() - new Date(bank.lastInterest).getTime()) / 3600000;
      if (hrs >= 24 && bank.balance > 0) { const i = Math.floor(bank.balance * 0.02); bank.balance += i; bank.lastInterest = new Date(); bank.history.push({ type: 'interest', amount: i, note: 'Interés diario 2%' }); }
    }
    if (action === 'deposit') {
      if (wallet.coins < amount) return res.status(400).json({ error: 'Saldo insuficiente' });
      const updated = await applyCoins(userId, -amount, false);
      bank.balance += amount; if (!bank.lastInterest) bank.lastInterest = new Date();
      bank.history.push({ type: 'deposit', amount, note: 'Depósito' }); await bank.save();
      res.json({ walletBalance: updated.coins, bankBalance: bank.balance });
    } else if (action === 'withdraw') {
      if (bank.balance < amount) return res.status(400).json({ error: 'Saldo insuficiente en banco' });
      bank.balance -= amount; bank.history.push({ type: 'withdraw', amount: -amount, note: 'Retiro' }); await bank.save();
      const updated = await applyCoins(userId, amount, false);
      res.json({ walletBalance: updated.coins, bankBalance: bank.balance });
    } else { res.status(400).json({ error: 'Acción inválida' }); }
  } catch (e) { console.error('[bank]', e); res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  PRÉSTAMOS WEB
// ══════════════════════════════════════════════════════════════════════════
app.get('/api/casino/loans/my', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try { res.json({ loans: await Loan.find({ userId: req.session.user.id, guildId: GUILD_ID }).sort({ createdAt: -1 }).lean() }); }
  catch { res.status(500).json({ error: 'Error' }); }
});
app.post('/api/casino/loans/request', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const { amount } = req.body;
    if (!amount || amount < 100 || amount > 50000) return res.status(400).json({ error: 'Monto entre 100 y 50,000' });
    if (await Loan.findOne({ userId, guildId: GUILD_ID, status: 'active' })) return res.status(400).json({ error: 'Ya tienes un préstamo activo' });
    const debt = Math.floor(amount * 1.05);
    await Loan.create({ userId, guildId: GUILD_ID, amount, debt, interestRate: 0.05, status: 'active' });
    const updated = await applyCoins(userId, amount, false);
    res.json({ newBalance: updated.coins, amount, debt });
  } catch { res.status(500).json({ error: 'Error' }); }
});
app.post('/api/casino/loans/pay', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const { loanId, amount } = req.body;
    const loan = await Loan.findOne({ _id: loanId, userId, guildId: GUILD_ID, status: 'active' });
    if (!loan) return res.status(404).json({ error: 'Préstamo no encontrado' });
    const wallet = await UserCoins.findOne({ userId, guildId: GUILD_ID });
    if (!wallet || wallet.coins < amount) return res.status(400).json({ error: 'Saldo insuficiente' });
    loan.debt -= amount; if (loan.debt <= 0) { loan.debt = 0; loan.status = 'paid'; } await loan.save();
    const updated = await applyCoins(userId, -amount, false);
    res.json({ loan, newBalance: updated.coins, paid: loan.status === 'paid' });
  } catch { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  MISIONES DIARIAS
// ══════════════════════════════════════════════════════════════════════════
const MISSION_POOL = [
  { id: 'play5',   name: 'Jugador activo',   desc: 'Juega 5 partidas',       target: 5,    reward: 200 },
  { id: 'play10',  name: 'Apostador nato',   desc: 'Juega 10 partidas',      target: 10,   reward: 400 },
  { id: 'win3',    name: 'Racha ganadora',   desc: 'Gana 3 partidas',        target: 3,    reward: 300 },
  { id: 'win5',    name: 'Campeón del día',  desc: 'Gana 5 partidas',        target: 5,    reward: 600 },
  { id: 'bet1000', name: 'Gran apostador',   desc: 'Apuesta 1,000 en total', target: 1000, reward: 150 },
  { id: 'bet5000', name: 'Alto riesgo',      desc: 'Apuesta 5,000 en total', target: 5000, reward: 500 },
  { id: 'slots3',  name: 'Maestro de slots', desc: 'Juega slots 3 veces',    target: 3,    reward: 150 },
  { id: 'trivia2', name: 'Cerebrito',        desc: 'Gana 2 trivias',         target: 2,    reward: 200 },
];
app.get('/api/casino/missions/daily', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const today = new Date().toISOString().split('T')[0];
    let daily = await DailyMission.findOne({ userId, guildId: GUILD_ID, date: today });
    if (!daily) {
      const picked = [...MISSION_POOL].sort(() => Math.random() - 0.5).slice(0, 3);
      daily = await DailyMission.create({ userId, guildId: GUILD_ID, date: today, missions: picked.map(m => ({ ...m })) });
    }
    res.json({ missions: daily.missions, date: today });
  } catch { res.status(500).json({ error: 'Error' }); }
});
app.post('/api/casino/missions/claim', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const today = new Date().toISOString().split('T')[0];
    const daily = await DailyMission.findOne({ userId, guildId: GUILD_ID, date: today });
    const mission = daily?.missions.find(m => m.id === req.body.missionId);
    if (!mission) return res.status(404).json({ error: 'Misión no encontrada' });
    if (!mission.completed) return res.status(400).json({ error: 'No completada' });
    if (mission.claimed) return res.status(400).json({ error: 'Ya reclamada' });
    mission.claimed = true; await daily.save();
    const updated = await applyCoins(userId, mission.reward, true);
    res.json({ reward: mission.reward, newBalance: updated.coins });
  } catch { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  TORNEO SEMANAL
// ══════════════════════════════════════════════════════════════════════════
function getCurrentWeek() {
  const now = new Date(), start = new Date(now.getFullYear(), 0, 1);
  return { week: Math.ceil(((now - start) / 86400000 + start.getDay() + 1) / 7), year: now.getFullYear() };
}
app.get('/api/casino/tournament', async (req, res) => {
  try {
    const { week, year } = getCurrentWeek();
    const weekStart = new Date(); weekStart.setDate(weekStart.getDate() - weekStart.getDay()); weekStart.setHours(0,0,0,0);
    const agg = await CasinoLog.aggregate([
      { $match: { guildId: GUILD_ID, timestamp: { $gte: weekStart } } },
      { $group: { _id: '$userId', totalProfit: { $sum: '$profit' }, totalBets: { $sum: 1 } } },
      { $sort: { totalProfit: -1 } }, { $limit: 10 }
    ]);
    const leaderboard = await Promise.all(agg.map(async (p, i) => {
      const u = await getUser(p._id);
      return { rank: i+1, userId: p._id, username: u.username, avatar: u.avatar, profit: p.totalProfit, bets: p.totalBets };
    }));
    res.json({ leaderboard, prizes: [{ place:1,reward:10000},{place:2,reward:5000},{place:3,reward:2500}], week, year });
  } catch { res.status(500).json({ error: 'Error' }); }
});
// Cron premios semanales (domingos a medianoche)
setInterval(async () => {
  const now = new Date();
  if (now.getDay() === 0 && now.getHours() === 0 && now.getMinutes() < 5) {
    try {
      const { week, year } = getCurrentWeek();
      const prevWeek = week === 1 ? 52 : week - 1;
      const prevYear = week === 1 ? year - 1 : year;
      if (await WeeklyTournament.findOne({ guildId: GUILD_ID, week: prevWeek, year: prevYear, ended: true })) return;
      const weekStart = new Date(); weekStart.setDate(weekStart.getDate() - 7);
      const agg = await CasinoLog.aggregate([{ $match: { guildId: GUILD_ID, timestamp: { $gte: weekStart } } }, { $group: { _id: '$userId', totalProfit: { $sum: '$profit' } } }, { $sort: { totalProfit: -1 } }, { $limit: 3 }]);
      const rewards = [10000, 5000, 2500];
      for (let i = 0; i < Math.min(agg.length, 3); i++) {
        await applyCoins(agg[i]._id, rewards[i], true);
        const u = await getUser(agg[i]._id);
        if (global.io) global.io.emit('tournament:winner', { place: i+1, username: u.username, reward: rewards[i] });
      }
      await WeeklyTournament.create({ guildId: GUILD_ID, week: prevWeek, year: prevYear, ended: true, endedAt: new Date(), prizes: agg.map((p,i) => ({ place:i+1,userId:p._id,amount:rewards[i],claimed:true })) });
    } catch (e) { console.error('[torneo cron]', e); }
  }
}, 300000);

// ══════════════════════════════════════════════════════════════════════════
//  INTERCAMBIO DE ARMAS
// ══════════════════════════════════════════════════════════════════════════
app.get('/api/casino/trades', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const trades = await TradeOffer.find({ guildId: GUILD_ID, $or: [{ fromUserId: userId }, { toUserId: userId }] }).sort({ createdAt: -1 }).limit(20).lean();
    const enriched = await Promise.all(trades.map(async t => {
      const [from, to] = await Promise.all([getUser(t.fromUserId), getUser(t.toUserId)]);
      return { ...t, fromUsername: from.username, fromAvatar: from.avatar, toUsername: to.username, toAvatar: to.avatar };
    }));
    res.json({ trades: enriched });
  } catch { res.status(500).json({ error: 'Error' }); }
});
app.post('/api/casino/trades/send', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const fromUserId = req.session.user.id;
    const { toUserId, fromArmId, toArmId, message } = req.body;
    if (fromUserId === toUserId) return res.status(400).json({ error: 'No puedes intercambiar contigo mismo' });
    const [fromArm, toArm] = await Promise.all([
      Arm.findOne({ _id: fromArmId, userId: fromUserId, guildId: GUILD_ID }),
      Arm.findOne({ _id: toArmId, userId: toUserId, guildId: GUILD_ID }),
    ]);
    if (!fromArm) return res.status(404).json({ error: 'Tu arma no encontrada' });
    if (!toArm) return res.status(404).json({ error: 'Arma del otro jugador no encontrada' });
    const trade = await TradeOffer.create({ guildId: GUILD_ID, fromUserId, toUserId, fromArmId: fromArm._id, toArmId: toArm._id, fromArmName: `${fromArm.name} ${fromArm.skin}`, toArmName: `${toArm.name} ${toArm.skin}`, message: message || '', expiresAt: new Date(Date.now() + 86400000) });
    if (global.io) { const u = await getUser(fromUserId); global.io.emit('trade:offer', { toUserId, fromUsername: u.username, fromArmName: trade.fromArmName, toArmName: trade.toArmName }); }
    res.json({ trade });
  } catch (e) { res.status(500).json({ error: e.message || 'Error' }); }
});
app.post('/api/casino/trades/respond', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const { tradeId, action } = req.body;
    const trade = await TradeOffer.findOne({ _id: tradeId, toUserId: userId, status: 'pending' });
    if (!trade) return res.status(404).json({ error: 'Oferta no encontrada' });
    if (action === 'accept') {
      const [fromArm, toArm] = await Promise.all([Arm.findOne({ _id: trade.fromArmId, userId: trade.fromUserId }), Arm.findOne({ _id: trade.toArmId, userId })]);
      if (!fromArm || !toArm) return res.status(400).json({ error: 'Una arma ya no está disponible' });
      fromArm.userId = userId; toArm.userId = trade.fromUserId;
      await Promise.all([fromArm.save(), toArm.save()]); trade.status = 'accepted';
    } else { trade.status = 'rejected'; }
    await trade.save();
    if (global.io) global.io.emit('trade:response', { fromUserId: trade.fromUserId, status: trade.status });
    res.json({ trade });
  } catch { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  CHAT EN VIVO
// ══════════════════════════════════════════════════════════════════════════
app.get('/api/casino/chat', async (req, res) => {
  try { res.json({ messages: await ChatMessage.find({ guildId: GUILD_ID }).sort({ timestamp: -1 }).limit(50).lean().then(m => m.reverse()) }); }
  catch { res.status(500).json({ error: 'Error' }); }
});
app.post('/api/casino/chat', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    if (await checkCasinoBan(userId)) return res.status(403).json({ error: 'Estás baneado del casino' });
    const { message } = req.body;
    if (!message || message.trim().length < 1 || message.length > 200) return res.status(400).json({ error: 'Mensaje inválido' });
    const u = req.session.user;
    const msg = await ChatMessage.create({ userId, guildId: GUILD_ID, username: u.username, avatar: u.avatar, message: message.trim().replace(/</g,'&lt;').replace(/>/g,'&gt;') });
    if (global.io) global.io.emit('chat:message', msg);
    res.json({ message: msg });
  } catch { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  PERFIL PÚBLICO
// ══════════════════════════════════════════════════════════════════════════
app.get('/api/casino/player/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const [coins, level, ach, arms, agg] = await Promise.all([
      UserCoins.findOne({ userId, guildId: GUILD_ID }).lean(),
      Level.findOne({ userId, guildId: GUILD_ID }).lean(),
      Achievements.findOne({ userId, guildId: GUILD_ID }).lean(),
      Arm.find({ userId, guildId: GUILD_ID }).sort({ sellPrice: -1 }).limit(6).lean(),
      CasinoLog.aggregate([{ $match: { userId, guildId: GUILD_ID } }, { $group: { _id: null, totalBets: { $sum: 1 }, netProfit: { $sum: '$profit' }, wins: { $sum: { $cond: [{ $gt: ['$profit', 0] }, 1, 0] } } } }]),
    ]);
    const u = await getUser(userId);
    const stats = agg[0] || { totalBets: 0, netProfit: 0, wins: 0 };
    res.json({ userId, username: u.username, avatar: u.avatar, coins: coins?.coins || 0, level: level?.level || 0, xp: level?.xp || 0, casinoWins: ach?.casino_wins || 0, totalBets: stats.totalBets, netProfit: stats.netProfit, winRate: stats.totalBets > 0 ? Math.round(stats.wins / stats.totalBets * 100) : 0, topArms: arms });
  } catch { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  SISTEMA DE AMIGOS
// ══════════════════════════════════════════════════════════════════════════
app.get('/api/casino/friends', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const accepted = await FriendRequest.find({ guildId: GUILD_ID, status: 'accepted', $or: [{ fromUserId: userId }, { toUserId: userId }] }).lean();
    const friendIds = accepted.map(f => f.fromUserId === userId ? f.toUserId : f.fromUserId);
    const friends = await Promise.all(friendIds.map(async id => { const u = await getUser(id); const c = await UserCoins.findOne({ userId: id, guildId: GUILD_ID }).lean(); return { userId: id, username: u.username, avatar: u.avatar, coins: c?.coins || 0 }; }));
    const pending = await FriendRequest.find({ guildId: GUILD_ID, toUserId: userId, status: 'pending' }).lean();
    const pendingEnriched = await Promise.all(pending.map(async p => { const u = await getUser(p.fromUserId); return { ...p, fromUsername: u.username, fromAvatar: u.avatar }; }));
    res.json({ friends, pending: pendingEnriched });
  } catch { res.status(500).json({ error: 'Error' }); }
});
app.post('/api/casino/friends/request', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const fromUserId = req.session.user.id;
    const { toUserId } = req.body;
    if (fromUserId === toUserId) return res.status(400).json({ error: 'No puedes agregarte a ti mismo' });
    if (await FriendRequest.findOne({ guildId: GUILD_ID, $or: [{ fromUserId, toUserId }, { fromUserId: toUserId, toUserId: fromUserId }] })) return res.status(400).json({ error: 'Solicitud ya existe' });
    const fr = await FriendRequest.create({ guildId: GUILD_ID, fromUserId, toUserId });
    if (global.io) { const u = await getUser(fromUserId); global.io.emit('friend:request', { toUserId, fromUsername: u.username, fromAvatar: u.avatar }); }
    res.json({ request: fr });
  } catch { res.status(500).json({ error: 'Error' }); }
});
app.post('/api/casino/friends/respond', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const fr = await FriendRequest.findOne({ _id: req.body.requestId, toUserId: req.session.user.id, status: 'pending' });
    if (!fr) return res.status(404).json({ error: 'Solicitud no encontrada' });
    fr.status = req.body.action === 'accept' ? 'accepted' : 'rejected';
    await fr.save(); res.json({ request: fr });
  } catch { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  ADMIN — BANS Y ALERTAS
// ══════════════════════════════════════════════════════════════════════════
app.get('/api/admin/casino/bans', requireAuth, async (req, res) => {
  try {
    const bans = await CasinoBan.find({ guildId: GUILD_ID }).sort({ createdAt: -1 }).lean();
    const enriched = await Promise.all(bans.map(async b => { const u = await getUser(b.userId); return { ...b, username: u.username, avatar: u.avatar }; }));
    res.json({ bans: enriched });
  } catch { res.status(500).json({ error: 'Error' }); }
});
app.post('/api/admin/casino/ban', requireAuth, async (req, res) => {
  try {
    const { userId, reason, hours } = req.body;
    const expiresAt = hours ? new Date(Date.now() + hours * 3600000) : null;
    await CasinoBan.findOneAndUpdate({ userId, guildId: GUILD_ID }, { userId, guildId: GUILD_ID, reason, bannedBy: req.session.user.id, expiresAt, createdAt: new Date() }, { upsert: true });
    if (global.io) global.io.emit('casino:banned', { userId });
    res.json({ success: true });
  } catch { res.status(500).json({ error: 'Error' }); }
});
app.post('/api/admin/casino/unban', requireAuth, async (req, res) => {
  try { await CasinoBan.deleteOne({ userId: req.body.userId, guildId: GUILD_ID }); res.json({ success: true }); }
  catch { res.status(500).json({ error: 'Error' }); }
});
app.get('/api/admin/casino/alerts', requireAuth, async (req, res) => {
  try {
    const since = new Date(Date.now() - 86400000);
    const alerts = await CasinoLog.find({ guildId: GUILD_ID, profit: { $gte: 50000 }, timestamp: { $gte: since } }).sort({ profit: -1 }).limit(20).lean();
    const enriched = await Promise.all(alerts.map(async a => { const u = await getUser(a.userId); return { ...a, username: u.username }; }));
    res.json({ alerts: enriched });
  } catch { res.status(500).json({ error: 'Error' }); }
});

// ══════════════════════════════════════════════════════════════════════════
//  CRON: Interés bancario cada hora
// ══════════════════════════════════════════════════════════════════════════
setInterval(async () => {
  try {
    const banks = await BankAccount.find({ guildId: GUILD_ID, balance: { $gt: 0 } });
    for (const bank of banks) {
      if (!bank.lastInterest) continue;
      if ((Date.now() - new Date(bank.lastInterest).getTime()) / 3600000 >= 24) {
        const interest = Math.floor(bank.balance * 0.02);
        bank.balance += interest; bank.lastInterest = new Date();
        bank.history.push({ type: 'interest', amount: interest, note: 'Interés diario 2%' }); await bank.save();
        await UserCoins.findOneAndUpdate({ userId: bank.userId, guildId: GUILD_ID }, { $inc: { coins: interest, totalEarned: interest } });
        const w = await UserCoins.findOne({ userId: bank.userId, guildId: GUILD_ID }).lean();
        if (w && global.io) global.io.emit('coins:update', { userId: bank.userId, coins: w.coins });
      }
    }
  } catch (e) { console.error('[bank cron]', e); }
}, 3600000);

// ══════════════════════════════════════════════════════════════════════════
//  WEBSOCKET
// ══════════════════════════════════════════════════════════════════════════
io.on('connection', socket => {
  console.log(`[WS] Conectado: ${socket.id}`);
  socket.on('disconnect', () => console.log(`[WS] Desconectado: ${socket.id}`));
});

global.emitUpdate = (event, data) => io.emit(event, data);

setInterval(async () => {
  const stats = await getStats();
  if (stats) io.emit('stats:update', stats);
}, 5000);

// ── PÁGINAS ───────────────────────────────────────────────────────────────
app.get('/login',     (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/dashboard', (req, res) => { if (!req.session?.user) return res.redirect('/login'); res.sendFile(path.join(__dirname, 'public', 'dashboard.html')); });
app.get('/casino',    (req, res) => res.sendFile(path.join(__dirname, 'public', 'casino.html')));
app.get('/',          (req, res) => res.redirect('/casino'));

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`[DASHBOARD] Puerto ${PORT}`));
