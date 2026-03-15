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

// ── Sirve fondos de perfil en /backgrounds/:bgId.png ──────────────────────
// Copia tus fondos con copy-arms.bat → public/backgrounds/
app.use('/backgrounds', express.static(path.join(__dirname, 'public', 'backgrounds')));

app.use(session({
  secret: process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex'),
  resave: false, saveUninitialized: false,
  cookie: { secure: false, maxAge: 24 * 60 * 60 * 1000 }
}));

mongoose.connect(process.env.MONGO_URI).then(() => console.log('[DB] Conectado')).catch(console.error);

// ── Schemas ────────────────────────────────────────────────────────────────
const { Schema, model } = mongoose;
const Level          = model('Level',          new Schema({ userId: String, guildId: String, xp: { type: Number, default: 0 }, level: { type: Number, default: 0 }, lastMessage: Date, streak: { type: Number, default: 0 }, lastMessageDate: Date, dailyChallenges: { date: String, messages: Number, messagesCompleted: Boolean, vcMinutes: Number, vcCompleted: Boolean } }));
const Warn           = model('Warn',           new Schema({ userId: String, guildId: String, moderator: String, reason: String, createdAt: { type: Date, default: Date.now } }));
const Mute           = model('Mute',           new Schema({ userId: String, guildId: String, expiresAt: Date, reason: String }));
const Giveaway       = model('Giveaway',       new Schema({ guildId: String, channelId: String, messageId: String, prize: String, winners: Number, hostedBy: String, endsAt: Date, ended: Boolean, participants: [String], winnerIds: [String], requiredRole: String, minLevel: Number, minInvites: Number, claimDeadline: Date, claimedBy: [String] }));
const GiveawayBlacklist = model('GiveawayBlacklist', new Schema({ guildId: String, userId: String, reason: { type: String, default: 'Sin razón' }, addedBy: String, addedAt: { type: Date, default: Date.now } }));
const GiveawayConfig = model('GiveawayConfig', new Schema({ guildId: { type: String, unique: true }, claimTimeout: { type: Number, default: 86400000 } }));
const AutoRole       = model('AutoRole',       new Schema({ guildId: String, panelId: { type: String, unique: true }, channelId: String, messageId: String, name: String, title: String, description: String, requireConfirm: { type: Boolean, default: true }, maxRoles: { type: Number, default: 0 }, roles: [{ roleId: String, emoji: String, label: String, description: String, order: Number, minLevel: { type: Number, default: 0 }, requiredRole: String }], createdAt: { type: Date, default: Date.now }, updatedAt: { type: Date, default: Date.now } }));
const SecurityLog    = model('SecurityLog',    new Schema({ userId: String, guildId: String, ip: String, country: String, countryCode: String, action: String, flagged: Boolean, joinedAt: { type: Date, default: Date.now } }));
const InviteTracker  = model('InviteTracker',  new Schema({ guildId: String, userId: String, code: String, uses: { type: Number, default: 0 }, total: { type: Number, default: 0 }, fake: { type: Number, default: 0 }, left: { type: Number, default: 0 } }));
const UserCoins      = model('UserCoins',      new Schema({ userId: String, guildId: String, coins: { type: Number, default: 0 }, totalEarned: { type: Number, default: 0 }, lastDaily: Date, lastWork: Date, ownedBackgrounds: { type: [String], default: [] }, activeBgId: { type: String, default: null } }));
const Loan           = model('Loan',           new Schema({ userId: String, guildId: String, amount: Number, debt: Number, interestRate: { type: Number, default: 0.01 }, status: { type: String, default: 'active' }, lastInterest: Date, createdAt: { type: Date, default: Date.now } }));
const ChannelsConfig = model('ChannelsConfig', new Schema({ guildId: String, logs: String, modLogs: String, welcome: String, levels: String, verification: String, tiktok: String, security: String, invites: String }));
const RewardsConfig  = model('RewardsConfig',  new Schema({ guildId: String, initialCoins: Number, trivia_easy: Number, trivia_medium: Number, trivia_hard: Number, misiones_reward: Number, shop_rol_custom: Number, shop_mention: Number, shop_vc_boost: Number, max_loss_per_day: Number, max_bets_per_hour: Number, logsChannelId: String }));
const GuildStyle     = model('GuildStyle',     new Schema({ guildId: { type: String, unique: true }, colors: { primary: Number, success: Number, warning: Number, danger: Number, info: Number, welcome: Number, giveaway: Number, level: Number }, footer: { text: String, iconUrl: String }, welcomeMessage: String, showTimestamp: { type: Boolean, default: true } }));
const CasinoLog      = model('CasinoLog',      new Schema({ userId: String, guildId: String, game: String, bet: Number, result: String, profit: Number, timestamp: { type: Date, default: Date.now } }));
const Achievements   = model('Achievements',   new Schema({ userId: String, guildId: String, badges: [{ id: String, name: String, unlockedAt: Date }], trivia_wins: { type: Number, default: 0 }, casino_wins: { type: Number, default: 0 }, win_streak: { type: Number, default: 0 }, max_bet: { type: Number, default: 0 }, createdAt: { type: Date, default: Date.now } }));
const MissionProgress= model('MissionProgress',new Schema({ userId: String, guildId: String, week: Number, year: Number, missions: [{ id: String, name: String, target: Number, progress: { type: Number, default: 0 }, completed: { type: Boolean, default: false }, reward: { type: Number, default: 300 } }], claimed: { type: Boolean, default: false }, claimedAt: Date, createdAt: { type: Date, default: Date.now } }));
const TriviaProgress = model('TriviaProgress', new Schema({ userId: String, guildId: String, totalWins: { type: Number, default: 0 }, totalFails: { type: Number, default: 0 }, streak: { type: Number, default: 0 }, triviaCount: { type: Number, default: 0 }, createdAt: { type: Date, default: Date.now } }));
const Trade          = model('Trade',          new Schema({ guildId: String, fromUserId: String, toUserId: String, fromCoins: { type: Number, default: 0 }, toCoins: { type: Number, default: 0 }, status: { type: String, default: 'pending' }, expiresAt: Date, createdAt: { type: Date, default: Date.now } }));
const ShopItem       = model('ShopItem',       new Schema({ guildId: String, itemId: String, name: String, description: String, price: Number, emoji: { type: String, default: '🛒' }, active: { type: Boolean, default: true }, createdBy: String, createdAt: { type: Date, default: Date.now } }));
const XpBoost        = model('XpBoost',        new Schema({ userId: String, guildId: String, multiplier: Number, expiresAt: Date, grantedBy: String }));
const GameHistory    = model('GameHistory',    new Schema({ guildId: String, game: String, result: mongoose.Schema.Types.Mixed, players: [{ userId: String, betType: String, amount: Number, won: Boolean, delta: Number }], endedAt: { type: Date, default: Date.now } }));
const ServerBackground=model('ServerBackground',new Schema({ guildId: String, bgId: String, name: String, description: String, price: { type: Number, default: 0 }, filePath: String, isDefault: { type: Boolean, default: false }, createdBy: String, createdAt: { type: Date, default: Date.now } }));
const Arm            = model('Arm',            new Schema({ userId: String, guildId: String, itemId: String, name: String, skin: String, type: String, emoji: String, rarity: String, rarityName: String, rarityEmoji: String, rarityColor: Number, wear: String, wearTag: String, basePrice: Number, sellPrice: Number, caseId: String, tradeId: String, obtainedAt: { type: Date, default: Date.now } }));
const MarketListing  = model('MarketListing',  new Schema({ guildId: String, sellerId: String, armId: mongoose.Schema.Types.ObjectId, itemId: String, name: String, skin: String, type: String, rarity: String, rarityName: String, rarityColor: Number, wear: String, wearTag: String, basePrice: Number, price: Number, status: { type: String, default: 'active' }, buyerId: String, listedAt: { type: Date, default: Date.now }, expiresAt: Date, soldAt: Date }));
const ArmCurrentPrice= model('ArmCurrentPrice',new Schema({ guildId: String, itemId: String, name: String, skin: String, rarity: String, rarityColor: Number, currentPrice: Number, openPrice24h: Number, high24h: Number, low24h: Number, volume24h: { type: Number, default: 0 }, lastUpdated: Date }));
const ArmPrice       = model('ArmPrice',       new Schema({ guildId: String, itemId: String, name: String, skin: String, rarity: String, open: Number, high: Number, low: Number, close: Number, volume: Number, timestamp: { type: Date, default: Date.now } }));
const MarketTx       = model('MarketTx',       new Schema({ guildId: String, itemId: String, name: String, skin: String, rarity: String, price: Number, sellerId: String, buyerId: String, timestamp: { type: Date, default: Date.now } }));

const ShopPurchase = mongoose.models.ShopPurchase || model('ShopPurchase', new Schema({
  userId: String, guildId: String, itemId: String, itemName: String,
  price: Number, purchasedAt: { type: Date, default: Date.now }
}));

// ── Discord helpers ────────────────────────────────────────────────────────
const GUILD_ID       = process.env.GUILD_ID;
const ADMIN_ROLE_IDS = (process.env.ADMIN_ROLE_IDS || process.env.ADMIN_ROLE_ID || '').split(',').map(r => r.trim()).filter(Boolean);
const userCache      = new Map();

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
  const data = { username: u?.username || userId, avatar: u?.avatar ? `https://cdn.discordapp.com/avatars/${userId}/${u.avatar}.png?size=32` : null };
  userCache.set(userId, data);
  return data;
}

function exchangeCode(code) {
  return new Promise(resolve => {
    const params = new URLSearchParams({ client_id: process.env.DISCORD_CLIENT_ID, client_secret: process.env.DISCORD_CLIENT_SECRET, grant_type: 'authorization_code', code, redirect_uri: process.env.DISCORD_REDIRECT_URI }).toString();
    const req = https.request({ hostname: 'discord.com', path: '/api/v10/oauth2/token', method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(params) } }, res => {
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

// ── Auth ───────────────────────────────────────────────────────────────────
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
      avatar: user.avatar ? `https://cdn.discordapp.com/avatars/${user.id}/${user.avatar}.png?size=64` : 'https://cdn.discordapp.com/embed/avatars/0.png',
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

// ── Stats helper ───────────────────────────────────────────────────────────
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
    return { members: guild?.approximate_member_count || 0, online: guild?.approximate_presence_count || 0, registeredUsers: totalUsers, activeGiveaways, activeLoans, flaggedJoins, totalWarns, joinsByDay, casinoBets: casinoAgg[0]?.bets || 0, casinoProfit: casinoAgg[0]?.profit || 0 };
  } catch (e) { console.error('[stats]', e); return null; }
}

// ══════════════════════════════════════════════════════════════════════════
//  APIs DEL DASHBOARD (requieren rol de admin)
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
      InviteTracker.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: '$userId', reales: { $sum: '$uses' }, total: { $sum: '$total' }, fake: { $sum: '$fake' }, left: { $sum: '$left' } } }, { $sort: { reales: -1 } }, { $skip: (page - 1) * limit }, { $limit: limit }]),
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

app.get('/api/casino/public/market', async (req, res) => {
  try { res.json({ items: await ArmCurrentPrice.find({ guildId: GUILD_ID }).sort({ currentPrice: -1 }).limit(30).lean() }); }
  catch (e) { res.status(500).json({ error: 'Error' }); }
});

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

app.get('/api/casino/public/shop', async (req, res) => {
  try { res.json({ items: await ShopItem.find({ guildId: GUILD_ID, active: true }).sort({ price: 1 }).lean() }); }
  catch (e) { res.status(500).json({ error: 'Error' }); }
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

// ── /api/casino/me/full — incluye activeBgId para el perfil del casino ─────
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
      coins:        coins?.coins        || 0,
      totalEarned:  coins?.totalEarned  || 0,
      activeBgId:   coins?.activeBgId   || null,   // ← fondo activo del usuario
      ownedBackgrounds: coins?.ownedBackgrounds || [],
      level:        level?.level        || 0,
      xp:           level?.xp           || 0,
      streak:       level?.streak       || 0,
      casinoWins:   ach?.casino_wins    || 0,
      registered:   !!coins,
      arms:         arms                || [],
    });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.post('/api/casino/play', async (req, res) => {
  const extGames = ['carreras','loteria','minas'];
  if (extGames.includes(req.body?.game)) return handleExtendedPlay(req, res);
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
      const reds = new Set([1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]);
      const blacks = new Set([2,4,6,8,10,11,13,15,17,20,22,24,26,28,29,31,33,35]);
      const checks = { rojo: reds.has(number), negro: blacks.has(number), par: number !== 0 && number % 2 === 0, impar: number !== 0 && number % 2 !== 0, bajo: number >= 1 && number <= 18, alto: number >= 19 && number <= 36, docena1: number >= 1 && number <= 12, docena2: number >= 13 && number <= 24, docena3: number >= 25 && number <= 36, verde: number === 0 };
      const mults = { rojo:2, negro:2, par:2, impar:2, bajo:2, alto:2, docena1:3, docena2:3, docena3:3, verde:35 };
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
    const delta = won ? winProfit : -bet;
    const updated = await UserCoins.findOneAndUpdate({ userId, guildId: GUILD_ID }, { $inc: { coins: delta, ...(won&&winProfit>0?{totalEarned:winProfit}:{}) } }, { new: true });
    await CasinoLog.create({ userId, guildId: GUILD_ID, game, bet, result: won?'win':'loss', profit: delta });
    if (won) await Achievements.findOneAndUpdate({ userId, guildId: GUILD_ID }, { $inc: { casino_wins: 1 }, $max: { max_bet: bet } }, { upsert: true });
    if (global.io) global.io.emit('coins:update', { userId, coins: updated.coins });
    res.json({ won, number, multiplier, reels: slotsReels, newBalance: updated.coins, profit: delta });
  } catch (e) { console.error('[casino/play]', e); res.status(500).json({ error: 'Error' }); }
});

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
    if (r < 0.50)      crashAt = 1 + Math.random() * 0.8;
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
    const updated = await UserCoins.findOneAndUpdate({ userId, guildId: GUILD_ID }, { $inc: { coins: winProfit, totalEarned: winProfit } }, { new: true });
    await CasinoLog.create({ userId, guildId: GUILD_ID, game: 'crash', bet, result: 'win', profit: winProfit });
    await Achievements.findOneAndUpdate({ userId, guildId: GUILD_ID }, { $inc: { casino_wins: 1 }, $max: { max_bet: bet } }, { upsert: true });
    if (global.io) global.io.emit('coins:update', { userId, coins: updated.coins });
    res.json({ won: true, newBalance: updated.coins, profit: winProfit });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

app.post('/api/casino/buy', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id, { itemId } = req.body;
    const item = await ShopItem.findOne({ guildId: GUILD_ID, itemId, active: true }).lean();
    if (!item) return res.status(404).json({ error: 'Item no encontrado' });
    const userCoins = await UserCoins.findOne({ userId, guildId: GUILD_ID });
    if (!userCoins || userCoins.coins < item.price) return res.status(400).json({ error: 'Saldo insuficiente' });
    const updated = await UserCoins.findOneAndUpdate({ userId, guildId: GUILD_ID }, { $inc: { coins: -item.price } }, { new: true });
    if (global.io) global.io.emit('coins:update', { userId, coins: updated.coins });
    await ShopPurchase.create({ userId, guildId: GUILD_ID, itemId, itemName: item.name, price: item.price }).catch(()=>{});
    res.json({ success: true, newBalance: updated.coins });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ── Carreras ───────────────────────────────────────────────────────────────
const HORSES_DATA = {
  A: { baseSpeed:0.35, variance:0.40 },
  B: { baseSpeed:0.38, variance:0.30 },
  C: { baseSpeed:0.32, variance:0.50 },
  D: { baseSpeed:0.40, variance:0.20 },
  E: { baseSpeed:0.30, variance:0.60 },
};

async function handleExtendedPlay(req, res) {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const { game, bet, horse, winner, result, profit: clientProfit } = req.body;
    if (!bet || bet < 1) return res.status(400).json({ error: 'Apuesta inválida' });
    const userCoins = await UserCoins.findOne({ userId, guildId: GUILD_ID });
    if (!userCoins) return res.status(403).json({ error: 'Usa /registrarse en Discord primero' });
    if (userCoins.coins < bet) return res.status(400).json({ error: 'Saldo insuficiente' });
    let won = false, delta = -bet;
    if (game === 'carreras') {
      const pos = Object.fromEntries(Object.keys(HORSES_DATA).map(k => [k, 0]));
      let serverWinner = null;
      for (let t = 0; t < 200 && !serverWinner; t++) {
        for (const [id, h] of Object.entries(HORSES_DATA)) {
          let adv = 0;
          if (Math.random() < h.baseSpeed + (Math.random()-0.5)*h.variance) adv = 1;
          if (Math.random() < 0.05) adv++;
          pos[id] = Math.min(20, pos[id] + adv);
        }
        const fin = Object.entries(pos).filter(([,v]) => v >= 20);
        if (fin.length) serverWinner = fin[0][0];
      }
      if (!serverWinner) serverWinner = winner;
      won = serverWinner === horse;
      delta = won ? bet : -bet;
    } else if (game === 'loteria') {
      const LOT_SYMS = ['🍒','⭐','💎','🍀','🔔','🎯','🎰','💰','🌙'];
      const grid = Array.from({length:9}, () => LOT_SYMS[Math.floor(Math.random()*LOT_SYMS.length)]);
      const counts = {}; for (const s of grid) counts[s] = (counts[s]||0)+1;
      const max = Math.max(...Object.values(counts));
      const serverMult = max>=7?3.0 : max>=5?2.0 : max>=4?1.5 : max>=3?1.0 : 0;
      won = serverMult > 0;
      delta = won ? Math.floor(bet * serverMult) - bet : -bet;
    } else if (game === 'minas') {
      if (result === 'cashout' && clientProfit > 0) {
        won = true; delta = Math.min(clientProfit, bet * 20);
      } else {
        won = false; delta = -bet;
      }
    } else {
      return res.status(400).json({ error: 'Juego no reconocido' });
    }
    const updated = await UserCoins.findOneAndUpdate({ userId, guildId: GUILD_ID }, { $inc: { coins: delta, ...(won&&delta>0?{totalEarned:delta}:{}) } }, { new: true });
    await CasinoLog.create({ userId, guildId: GUILD_ID, game, bet, result: won?'win':'loss', profit: delta });
    if (won) await Achievements.findOneAndUpdate({ userId, guildId: GUILD_ID }, { $inc:{casino_wins:1}, $max:{max_bet:bet} }, { upsert:true });
    if (global.io) global.io.emit('coins:update', { userId, coins: updated.coins });
    res.json({ won, profit: delta, newBalance: updated.coins });
  } catch (e) { console.error('[casino/extended]', e); res.status(500).json({ error: 'Error' }); }
}

// ── Batalla ────────────────────────────────────────────────────────────────
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
        CasinoLog.create({ userId, guildId: GUILD_ID, game:'batalla', bet, result:iWon?'win':'loss', profit:iWon?bet:-bet }),
        CasinoLog.create({ userId:opponentId, guildId: GUILD_ID, game:'batalla', bet, result:iWon?'loss':'win', profit:iWon?-bet:bet }),
      ]);
      if (iWon) await Achievements.findOneAndUpdate({ userId, guildId: GUILD_ID }, { $inc:{casino_wins:1}, $max:{max_bet:bet} }, { upsert:true });
    }
    if (global.io) { global.io.emit('coins:update', { userId, coins: myCoins.coins }); global.io.emit('coins:update', { userId: opponentId, coins: opCoins.coins }); }
    res.json({ won: iWon, tie, myRoll, opponentRoll: opRoll, newBalance: myCoins.coins });
  } catch (e) { console.error('[battle]', e); res.status(500).json({ error: 'Error' }); }
});

// ── Trivia ─────────────────────────────────────────────────────────────────
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
const TRIVIA_REWARDS = { easy:10, medium:20, hard:35 };

app.get('/api/casino/trivia/questions', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id;
    const coins = await UserCoins.findOne({ userId, guildId: GUILD_ID }).lean();
    if (!coins) return res.status(403).json({ error: 'Usa /registrarse primero' });
    const pick = (d, n) => TRIVIA_POOL.filter(q=>q.d===d).sort(()=>Math.random()-.5).slice(0,n);
    const questions = [...pick('easy',2),...pick('medium',2),...pick('hard',2)].sort(()=>Math.random()-.5).map(q => ({ q:q.q, o:q.o, d:q.d }));
    req.session.triviaSession = {
      questions: questions.map(qq => { const orig = TRIVIA_POOL.find(p=>p.q===qq.q); return { ...qq, c: orig?.c ?? 0 }; }),
      answered: [], ts: Date.now(),
    };
    res.json({ questions });
  } catch(e) { res.status(500).json({ error:'Error' }); }
});

app.post('/api/casino/trivia/answer', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  try {
    const userId = req.session.user.id, { idx, answer } = req.body, ts = req.session.triviaSession;
    if (!ts || Date.now()-ts.ts > 600000) return res.status(400).json({ error:'Sesión expirada' });
    if (ts.answered.includes(idx)) return res.status(400).json({ error:'Ya respondida' });
    ts.answered.push(idx);
    const q = ts.questions[idx], correct = answer === q.c, reward = correct ? TRIVIA_REWARDS[q.d] || 10 : 0;
    let newBalance = 0;
    if (correct && reward > 0) {
      const updated = await UserCoins.findOneAndUpdate({ userId, guildId: GUILD_ID }, { $inc: { coins: reward, totalEarned: reward } }, { new: true });
      newBalance = updated?.coins || 0;
      if (global.io) global.io.emit('coins:update', { userId, coins: newBalance });
    } else {
      const uc = await UserCoins.findOne({ userId, guildId: GUILD_ID }).lean();
      newBalance = uc?.coins || 0;
    }
    await TriviaProgress.findOneAndUpdate({ userId, guildId: GUILD_ID }, { $inc: { totalWins: correct?1:0, totalFails: correct?0:1 } }, { upsert: true });
    res.json({ correct, correctAnswer: q.c, reward, newBalance });
  } catch(e) { res.status(500).json({ error:'Error' }); }
});

// ── Cajas ──────────────────────────────────────────────────────────────────
const CASES_CONFIG = {
  iniciacion: { price:800,   rarities:['consumer','industrial','milspec'],    weights:[7992,1598,320], specialChance:0  },
  clasificada:{ price:2000,  rarities:['industrial','milspec','restricted'],  weights:[1598,320,64],   specialChance:0  },
  elite:      { price:5000,  rarities:['milspec','restricted','classified'],  weights:[320,64,13],     specialChance:1  },
  dorada:     { price:12000, rarities:['restricted','classified','covert'],   weights:[64,13,3],       specialChance:3  },
  contrabando:{ price:25000, rarities:['classified','covert','special'],      weights:[13,3,1],        specialChance:8  },
};
const RARITY_ITEMS = {
  consumer:  [{id:'p250_sand_dune',name:'P250',skin:'Sand Dune',bp:500},{id:'glock_sand_dune',name:'Glock-18',skin:'Sand Dune',bp:450},{id:'famas_doomkitty',name:'FAMAS',skin:'Doomkitty',bp:800},{id:'aug_storm',name:'AUG',skin:'Storm',bp:680},{id:'m249_system_lock',name:'M249',skin:'System Lock',bp:490}],
  industrial:[{id:'ak47_blue_laminate',name:'AK-47',skin:'Blue Laminate',bp:2200},{id:'ak47_jungle_spray',name:'AK-47',skin:'Jungle Spray',bp:2500},{id:'awp_safari_mesh',name:'AWP',skin:'Safari Mesh',bp:3800},{id:'deagle_midnight_storm',name:'Desert Eagle',skin:'Midnight Storm',bp:2400},{id:'bizon_osiris',name:'PP-Bizon',skin:'Osiris',bp:1600}],
  milspec:   [{id:'ak47_neon_rider',name:'AK-47',skin:'Neon Rider',bp:10000},{id:'deagle_printstream',name:'Desert Eagle',skin:'Printstream',bp:13000},{id:'awp_chromatic',name:'AWP',skin:'Chromatic Aberration',bp:14000},{id:'m4a1_flashback',name:'M4A1-S',skin:'Flashback',bp:12000},{id:'five7_hyper_beast',name:'Five-SeveN',skin:'Hyper Beast',bp:9500}],
  restricted:[{id:'ak47_wasteland',name:'AK-47',skin:'Wasteland Rebel',bp:35000},{id:'awp_pit_viper',name:'AWP',skin:'Pit Viper',bp:50000},{id:'m4a1_nightmare',name:'M4A1-S',skin:'Nightmare',bp:45000},{id:'deagle_golden_koi',name:'Desert Eagle',skin:'Golden Koi',bp:32000},{id:'usp_neo_noir',name:'USP-S',skin:'Neo-Noir',bp:38000}],
  classified:[{id:'ak47_wild_lotus',name:'AK-47',skin:'Wild Lotus',bp:150000},{id:'m4a4_howl',name:'M4A4',skin:'Howl',bp:180000},{id:'awp_medusa',name:'AWP',skin:'Medusa',bp:170000},{id:'deagle_blaze',name:'Desert Eagle',skin:'Blaze',bp:130000},{id:'usp_kill_confirmed',name:'USP-S',skin:'Kill Confirmed',bp:140000}],
  covert:    [{id:'ak47_fire_serpent',name:'AK-47',skin:'Fire Serpent',bp:550000},{id:'awp_dragon_lore',name:'AWP',skin:'Dragon Lore',bp:600000},{id:'awp_gungnir',name:'AWP',skin:'Gungnir',bp:520000},{id:'m4a4_poseidon',name:'M4A4',skin:'Poseidon',bp:420000},{id:'ak47_case_hardened',name:'AK-47',skin:'Case Hardened',bp:350000}],
  special:   [{id:'knife_karambit_doppler',name:'Karambit',skin:'Doppler Fase 2',bp:1200000},{id:'knife_butterfly_tiger',name:'Butterfly Knife',skin:'Tiger Tooth',bp:1000000},{id:'knife_m9_fade',name:'M9 Bayoneta',skin:'Fade',bp:950000},{id:'gloves_sport_pandeo',name:'Sport Gloves',skin:'Pandeo',bp:1500000},{id:'knife_bayonet_slaughter',name:'Bayoneta',skin:'Slaughter',bp:800000}],
};
const WEARS_DATA  = [{name:'Factory New',tag:'FN',mult:1.0},{name:'Minimal Wear',tag:'MW',mult:0.80},{name:'Field-Tested',tag:'FT',mult:0.60},{name:'Well-Worn',tag:'WW',mult:0.40},{name:'Battle-Scarred',tag:'BS',mult:0.20}];
const WEAR_W      = [10,24,33,24,9];
const RARITY_META = {consumer:{color:0x9ea3b0,sellMult:0.55},industrial:{color:0x5e98d9,sellMult:0.55},milspec:{color:0x4b69ff,sellMult:0.55},restricted:{color:0x8847ff,sellMult:0.60},classified:{color:0xd32ce6,sellMult:0.60},covert:{color:0xeb4b4b,sellMult:0.65},special:{color:0xffd700,sellMult:0.70}};

function rollCase(caseId) {
  const box = CASES_CONFIG[caseId]; if(!box) return null;
  let rarity;
  const sr = Math.random()*100;
  if (sr < box.specialChance) { rarity='special'; }
  else {
    const tw = box.weights.reduce((a,b)=>a+b,0); let r=Math.random()*tw;
    rarity = box.rarities[box.rarities.length-1];
    for(let i=0;i<box.rarities.length;i++){ r-=box.weights[i]; if(r<=0){rarity=box.rarities[i];break;} }
  }
  const pool = RARITY_ITEMS[rarity], item = pool[Math.floor(Math.random()*pool.length)];
  const tw2 = WEAR_W.reduce((a,b)=>a+b,0); let wr=Math.random()*tw2, wi=WEAR_W.length-1;
  for(let i=0;i<WEAR_W.length;i++){wr-=WEAR_W[i];if(wr<=0){wi=i;break;}}
  const wear = WEARS_DATA[wi], rm = RARITY_META[rarity];
  return { itemId:item.id, name:item.name, skin:item.skin, type:'rifle', emoji:'🔫', rarity, rarityName:rarity, rarityColor:rm.color, wear:wear.name, wearTag:wear.tag, basePrice:item.bp, sellPrice:Math.floor(item.bp*wear.mult*rm.sellMult), caseId };
}

app.post('/api/casino/case/open', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error:'No autenticado' });
  try {
    const userId = req.session.user.id, { caseId } = req.body;
    const box = CASES_CONFIG[caseId];
    if (!box) return res.status(400).json({ error:'Caja inválida' });
    const uc = await UserCoins.findOne({ userId, guildId:GUILD_ID });
    if (!uc) return res.status(403).json({ error:'Usa /registrarse primero' });
    if (uc.coins < box.price) return res.status(400).json({ error:'Saldo insuficiente' });
    const drop = rollCase(caseId);
    if (!drop) return res.status(500).json({ error:'Error generando drop' });
    await Promise.all([
      UserCoins.findOneAndUpdate({userId,guildId:GUILD_ID},{$inc:{coins:-box.price}},{new:true}),
      Arm.create({userId,guildId:GUILD_ID,...drop,obtainedAt:new Date()}),
    ]);
    const updated = await UserCoins.findOne({userId,guildId:GUILD_ID}).lean();
    if (global.io) global.io.emit('coins:update',{userId,coins:updated.coins});
    res.json({ drop, newBalance:updated.coins });
  } catch(e){ console.error('[case]',e); res.status(500).json({error:'Error'}); }
});

app.get('/api/casino/inventory', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error:'No autenticado' });
  try {
    const userId = req.session.user.id;
    const arms = await Arm.find({userId,guildId:GUILD_ID}).sort({obtainedAt:-1}).lean();
    res.json({ arms });
  } catch(e){ res.status(500).json({error:'Error'}); }
});

app.post('/api/casino/market/buy', async (req, res) => {
  if (!req.session?.user) return res.status(401).json({ error:'No autenticado' });
  try {
    const userId = req.session.user.id, { listingId } = req.body;
    const listing = await MarketListing.findOne({_id:listingId,guildId:GUILD_ID,status:'active'});
    if (!listing) return res.status(404).json({ error:'Listing no encontrado o ya vendido' });
    if (listing.sellerId === userId) return res.status(400).json({ error:'No puedes comprarte a ti mismo' });
    const uc = await UserCoins.findOne({userId,guildId:GUILD_ID});
    if (!uc || uc.coins < listing.price) return res.status(400).json({ error:'Saldo insuficiente' });
    await Promise.all([
      UserCoins.findOneAndUpdate({userId,guildId:GUILD_ID},{$inc:{coins:-listing.price}}),
      UserCoins.findOneAndUpdate({userId:listing.sellerId,guildId:GUILD_ID},{$inc:{coins:listing.price,totalEarned:listing.price}}),
      MarketListing.findByIdAndUpdate(listingId,{status:'sold',buyerId:userId,soldAt:new Date()}),
      Arm.findOneAndUpdate({_id:listing.armId},{userId,tradeId:null}),
      MarketTx.create({guildId:GUILD_ID,itemId:listing.itemId,name:listing.name,skin:listing.skin,rarity:listing.rarity,price:listing.price,sellerId:listing.sellerId,buyerId:userId}),
    ]);
    const updated = await UserCoins.findOne({userId,guildId:GUILD_ID}).lean();
    if (global.io) { global.io.emit('coins:update',{userId,coins:updated.coins}); global.io.emit('market:update',{}); }
    res.json({ success:true, newBalance:updated.coins });
  } catch(e){ console.error('[market/buy]',e); res.status(500).json({error:'Error'}); }
});

app.get('/api/admin/purchases', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, limit=15;
    const [agg, count] = await Promise.all([
      ShopPurchase.find({guildId:GUILD_ID}).sort({purchasedAt:-1}).skip((page-1)*limit).limit(limit).lean(),
      ShopPurchase.countDocuments({guildId:GUILD_ID}),
    ]);
    const enriched = await Promise.all(agg.map(async p => { const u = await getUser(p.userId); return {...p, username:u.username, avatar:u.avatar}; }));
    res.json({ items:enriched, pages:Math.ceil(count/limit) });
  } catch(e){ res.status(500).json({error:'Error'}); }
});

// ── WebSocket ──────────────────────────────────────────────────────────────
io.on('connection', socket => {
  console.log(`[WS] Conectado: ${socket.id}`);
  socket.on('disconnect', () => console.log(`[WS] Desconectado: ${socket.id}`));
});

global.emitUpdate = (event, data) => io.emit(event, data);

setInterval(async () => {
  const stats = await getStats();
  if (stats) io.emit('stats:update', stats);
}, 5000);

// ── Páginas ────────────────────────────────────────────────────────────────
app.get('/login',     (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/dashboard', (req, res) => { if (!req.session?.user) return res.redirect('/login'); res.sendFile(path.join(__dirname, 'public', 'dashboard.html')); });
app.get('/casino',    (req, res) => res.sendFile(path.join(__dirname, 'public', 'casino.html')));
app.get('/',          (req, res) => res.redirect('/casino'));

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`[DASHBOARD] Puerto ${PORT}`));