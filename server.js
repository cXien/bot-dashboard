require('dotenv').config();
const express  = require('express');
const session  = require('express-session');
const mongoose = require('mongoose');
const https    = require('https');
const path     = require('path');
const crypto   = require('crypto');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use(session({
  secret: process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex'),
  resave: false, saveUninitialized: false,
  cookie: { secure: false, maxAge: 24 * 60 * 60 * 1000 }
}));

mongoose.connect(process.env.MONGO_URI).then(() => console.log('[DB] Conectado')).catch(console.error);

// ── Schemas ────────────────────────────────────────────────────────────────
const { Schema, model } = mongoose;
const Level          = model('Level',          new Schema({ userId: String, guildId: String, xp: { type: Number, default: 0 }, level: { type: Number, default: 0 }, lastMessage: Date }));
const Warn           = model('Warn',           new Schema({ userId: String, guildId: String, moderator: String, reason: String, createdAt: { type: Date, default: Date.now } }));
const Mute           = model('Mute',           new Schema({ userId: String, guildId: String, expiresAt: Date, reason: String }));
const Giveaway       = model('Giveaway',       new Schema({ guildId: String, channelId: String, messageId: String, prize: String, winners: Number, hostedBy: String, endsAt: Date, ended: Boolean, participants: [String], winnerIds: [String], requiredRole: String, minLevel: Number, minInvites: Number, claimDeadline: Date, claimedBy: [String] }));
const SecurityLog    = model('SecurityLog',    new Schema({ userId: String, guildId: String, ip: String, country: String, countryCode: String, action: String, flagged: Boolean, joinedAt: { type: Date, default: Date.now } }));
const InviteTracker  = model('InviteTracker',  new Schema({ guildId: String, userId: String, code: String, uses: { type: Number, default: 0 }, total: { type: Number, default: 0 }, fake: { type: Number, default: 0 }, left: { type: Number, default: 0 } }));
const UserCoins      = model('UserCoins',      new Schema({ userId: String, guildId: String, coins: { type: Number, default: 0 }, totalEarned: { type: Number, default: 0 }, lastDaily: Date, lastWork: Date }));
const Loan           = model('Loan',           new Schema({ userId: String, guildId: String, amount: Number, debt: Number, interestRate: { type: Number, default: 0.01 }, status: { type: String, default: 'active' }, lastInterest: Date, createdAt: { type: Date, default: Date.now } }));
const ChannelsConfig = model('ChannelsConfig', new Schema({ guildId: String, logs: String, modLogs: String, welcome: String, levels: String, verification: String, tiktok: String, security: String, invites: String }));
const RewardsConfig  = model('RewardsConfig',  new Schema({ guildId: String, initialCoins: Number, trivia_easy: Number, trivia_medium: Number, trivia_hard: Number, misiones_reward: Number, max_loss_per_day: Number, max_bets_per_hour: Number, logsChannelId: String }));
const CasinoLog      = model('CasinoLog',      new Schema({ userId: String, guildId: String, game: String, bet: Number, result: String, profit: Number, timestamp: { type: Date, default: Date.now } }));

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
    if (!member?.roles) return res.redirect('/login?error=not_in_server');
    if (!member.roles.some(r => ADMIN_ROLE_IDS.includes(r))) return res.redirect('/login?error=no_permission');
    req.session.user = { id: user.id, username: user.username, avatar: user.avatar ? `https://cdn.discordapp.com/avatars/${user.id}/${user.avatar}.png?size=64` : 'https://cdn.discordapp.com/embed/avatars/0.png' };
    res.redirect('/dashboard');
  } catch (e) { console.error('[AUTH]', e); res.redirect('/login?error=server_error'); }
});

app.get('/auth/logout', (req, res) => { req.session.destroy(); res.redirect('/login'); });
app.get('/api/me', requireAuth, (req, res) => res.json(req.session.user));

// ── API: Stats ─────────────────────────────────────────────────────────────
app.get('/api/stats', requireAuth, async (req, res) => {
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

    res.json({
      members: guild?.approximate_member_count || 0,
      online:  guild?.approximate_presence_count || 0,
      registeredUsers: totalUsers,
      activeGiveaways, activeLoans, flaggedJoins,
      totalWarns, joinsByDay,
      casinoBets:   casinoAgg[0]?.bets   || 0,
      casinoProfit: casinoAgg[0]?.profit  || 0,
    });
  } catch (e) { console.error('[stats]', e); res.status(500).json({ error: 'Error' }); }
});

// ── API: Invitaciones ──────────────────────────────────────────────────────
app.get('/api/invites', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = 15;
    const [agg, countAgg] = await Promise.all([
      InviteTracker.aggregate([
        { $match: { guildId: GUILD_ID } },
        { $group: { _id: '$userId', reales: { $sum: '$uses' }, total: { $sum: '$total' }, fake: { $sum: '$fake' }, left: { $sum: '$left' } } },
        { $sort: { reales: -1 } },
        { $skip: (page - 1) * limit },
        { $limit: limit },
      ]),
      InviteTracker.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: '$userId' } }, { $count: 'total' }]),
    ]);
    const total = countAgg[0]?.total || 0;
    const enriched = await Promise.all(agg.map(async (inv, i) => {
      const u = await getUser(inv._id);
      return { rank: (page - 1) * limit + i + 1, userId: inv._id, username: u.username, avatar: u.avatar, reales: inv.reales, total: inv.total, fake: inv.fake, left: inv.left };
    }));
    res.json({ invites: enriched, total, pages: Math.ceil(total / limit) });
  } catch (e) { console.error('[invites]', e); res.status(500).json({ error: 'Error' }); }
});

// ── API: Sorteos ───────────────────────────────────────────────────────────
app.get('/api/giveaways', requireAuth, async (req, res) => {
  try {
    const { status = 'all', page = 1 } = req.query;
    const filter = { guildId: GUILD_ID };
    if (status === 'active') filter.ended = false;
    if (status === 'ended')  filter.ended = true;
    const [giveaways, total] = await Promise.all([
      Giveaway.find(filter).sort({ endsAt: -1 }).skip((page - 1) * 10).limit(10).lean(),
      Giveaway.countDocuments(filter),
    ]);
    const enriched = await Promise.all(giveaways.map(async g => {
      const h = await getUser(g.hostedBy);
      return { ...g, hostedByName: h.username, hostedByAvatar: h.avatar };
    }));
    res.json({ giveaways: enriched, total, pages: Math.ceil(total / 10) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ── API: Economía ──────────────────────────────────────────────────────────
app.get('/api/economy', requireAuth, async (req, res) => {
  try {
    const { sort = 'coins', page = 1 } = req.query;
    const limit = 15;
    let items, total;
    if (sort === 'level') {
      [items, total] = await Promise.all([Level.find({ guildId: GUILD_ID }).sort({ level: -1, xp: -1 }).skip((page - 1) * limit).limit(limit).lean(), Level.countDocuments({ guildId: GUILD_ID })]);
    } else {
      [items, total] = await Promise.all([UserCoins.find({ guildId: GUILD_ID }).sort({ coins: -1 }).skip((page - 1) * limit).limit(limit).lean(), UserCoins.countDocuments({ guildId: GUILD_ID })]);
    }
    const enriched = await Promise.all(items.map(async (u, i) => {
      const d = await getUser(u.userId);
      return { rank: (page - 1) * limit + i + 1, userId: u.userId, username: d.username, avatar: d.avatar, coins: u.coins || 0, totalEarned: u.totalEarned || 0, level: u.level || 0, xp: u.xp || 0 };
    }));
    res.json({ users: enriched, total, pages: Math.ceil(total / limit) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ── API: Préstamos ─────────────────────────────────────────────────────────
app.get('/api/loans', requireAuth, async (req, res) => {
  try {
    const { status = 'all', page = 1 } = req.query;
    const filter = { guildId: GUILD_ID };
    if (status !== 'all') filter.status = status;
    const [loans, total] = await Promise.all([
      Loan.find(filter).sort({ createdAt: -1 }).skip((page - 1) * 15).limit(15).lean(),
      Loan.countDocuments(filter),
    ]);
    const enriched = await Promise.all(loans.map(async l => {
      const u = await getUser(l.userId);
      return { ...l, username: u.username, avatar: u.avatar };
    }));
    res.json({ loans: enriched, total, pages: Math.ceil(total / 15) });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ── API: Moderación ────────────────────────────────────────────────────────
app.get('/api/moderation', requireAuth, async (req, res) => {
  try {
    const { tab = 'warns', page = 1 } = req.query;
    const limit = 15;

    if (tab === 'warns') {
      const [items, total] = await Promise.all([Warn.find({ guildId: GUILD_ID }).sort({ createdAt: -1 }).skip((page - 1) * limit).limit(limit).lean(), Warn.countDocuments({ guildId: GUILD_ID })]);
      const enriched = await Promise.all(items.map(async w => { const [u, m] = await Promise.all([getUser(w.userId), getUser(w.moderator)]); return { ...w, username: u.username, avatar: u.avatar, modName: m.username }; }));
      return res.json({ items: enriched, total, pages: Math.ceil(total / limit) });
    }
    if (tab === 'mutes') {
      const [items, total] = await Promise.all([Mute.find({ guildId: GUILD_ID }).sort({ _id: -1 }).skip((page - 1) * limit).limit(limit).lean(), Mute.countDocuments({ guildId: GUILD_ID })]);
      const enriched = await Promise.all(items.map(async m => { const u = await getUser(m.userId); return { ...m, username: u.username, avatar: u.avatar }; }));
      return res.json({ items: enriched, total, pages: Math.ceil(total / limit) });
    }
    if (tab === 'logs') {
      const [items, total] = await Promise.all([SecurityLog.find({ guildId: GUILD_ID }).sort({ joinedAt: -1 }).skip((page - 1) * limit).limit(limit).lean(), SecurityLog.countDocuments({ guildId: GUILD_ID })]);
      const enriched = await Promise.all(items.map(async l => { const u = await getUser(l.userId); return { ...l, username: u.username, avatar: u.avatar }; }));
      return res.json({ items: enriched, total, pages: Math.ceil(total / limit) });
    }
    res.json({ items: [], total: 0, pages: 0 });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ── API: Niveles ───────────────────────────────────────────────────────────
app.get('/api/levels', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = 15;
    const [items, total, dist] = await Promise.all([
      Level.find({ guildId: GUILD_ID }).sort({ level: -1, xp: -1 }).skip((page - 1) * limit).limit(limit).lean(),
      Level.countDocuments({ guildId: GUILD_ID }),
      Level.aggregate([{ $match: { guildId: GUILD_ID } }, { $bucket: { groupBy: '$level', boundaries: [0, 1, 5, 10, 20, 50], default: '50+', output: { count: { $sum: 1 } } } }]),
    ]);
    const enriched = await Promise.all(items.map(async (u, i) => {
      const d = await getUser(u.userId);
      return { rank: (page - 1) * limit + i + 1, userId: u.userId, username: d.username, avatar: d.avatar, level: u.level || 0, xp: u.xp || 0 };
    }));
    res.json({ users: enriched, total, pages: Math.ceil(total / limit), distribution: dist });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ── API: Configuración ─────────────────────────────────────────────────────
app.get('/api/config', requireAuth, async (req, res) => {
  try {
    const [channels, rewards, guild, guildChannels] = await Promise.all([
      ChannelsConfig.findOne({ guildId: GUILD_ID }).lean(),
      RewardsConfig.findOne({ guildId: GUILD_ID }).lean(),
      discordReq(`/guilds/${GUILD_ID}`, null, true),
      discordReq(`/guilds/${GUILD_ID}/channels`, null, true),
    ]);
    const channelMap = {};
    if (Array.isArray(guildChannels)) guildChannels.forEach(c => { channelMap[c.id] = c.name; });
    const rc = id => id ? { id, name: channelMap[id] ? `#${channelMap[id]}` : `#${id}` } : null;
    res.json({
      guildName: guild?.name || 'Servidor',
      guildIcon: guild?.icon ? `https://cdn.discordapp.com/icons/${GUILD_ID}/${guild.icon}.png?size=64` : null,
      memberCount: guild?.approximate_member_count || 0,
      channels: channels ? { logs: rc(channels.logs), modLogs: rc(channels.modLogs), welcome: rc(channels.welcome), levels: rc(channels.levels), verification: rc(channels.verification), tiktok: rc(channels.tiktok), security: rc(channels.security), invites: rc(channels.invites) } : null,
      rewards: rewards || null,
    });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ── API: Bans ──────────────────────────────────────────────────────────────
app.get('/api/bans', requireAuth, async (req, res) => {
  try {
    const bans = await discordReq(`/guilds/${GUILD_ID}/bans?limit=100`, null, true);
    if (!Array.isArray(bans)) return res.json({ bans: [], total: 0, pages: 0 });
    const page = parseInt(req.query.page) || 1;
    const limit = 15;
    const slice = bans.slice((page - 1) * limit, page * limit);
    res.json({
      bans: slice.map(b => ({ userId: b.user.id, username: b.user.username, avatar: b.user.avatar ? `https://cdn.discordapp.com/avatars/${b.user.id}/${b.user.avatar}.png?size=32` : null, reason: b.reason || 'Sin razón' })),
      total: bans.length, pages: Math.ceil(bans.length / limit),
    });
  } catch (e) { res.status(500).json({ error: 'Error' }); }
});

// ── Páginas ────────────────────────────────────────────────────────────────
app.get('/login',     (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/dashboard', (req, res) => { if (!req.session?.user) return res.redirect('/login'); res.sendFile(path.join(__dirname, 'public', 'dashboard.html')); });
app.get('/',          (req, res) => res.redirect(req.session?.user ? '/dashboard' : '/login'));

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`[DASHBOARD] Puerto ${PORT}`));