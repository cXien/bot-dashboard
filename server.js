require('dotenv').config();
const express  = require('express');
const session  = require('express-session');
const mongoose = require('mongoose');
const https    = require('https');
const path     = require('path');
const crypto   = require('crypto');
const http     = require('http');
const socketIo = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, { cors: { origin: '*' } });

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
const Arm            = model('Arm',            new Schema({ userId: String, guildId: String, itemId: String, name: String, skin: String, type: String, emoji: String, rarity: String, rarityName: String, rarityEmoji: String, rarityColor: Number, wear: String, wearTag: String, basePrice: Number, sellPrice: Number, caseId: String, tradeId: String, obtainedAt: { type: Date, default: Date.now } }));
const MarketListing  = model('MarketListing',  new Schema({ guildId: String, sellerId: String, itemId: String, name: String, skin: String, rarity: String, rarityName: String, rarityColor: Number, wear: String, wearTag: String, price: Number, status: String, buyerId: String, listedAt: { type: Date, default: Date.now }, expiresAt: Date, soldAt: Date }));
const ArmCurrentPrice= model('ArmCurrentPrice',new Schema({ guildId: String, itemId: String, name: String, skin: String, rarity: String, rarityColor: Number, currentPrice: Number, openPrice24h: Number, high24h: Number, low24h: Number, volume24h: { type: Number, default: 0 }, lastUpdated: Date }));
const ArmPrice       = model('ArmPrice',       new Schema({ guildId: String, itemId: String, name: String, skin: String, rarity: String, open: Number, high: Number, low: Number, close: Number, volume: Number, timestamp: { type: Date, default: Date.now } }));
const MarketTx       = model('MarketTx',       new Schema({ guildId: String, itemId: String, name: String, skin: String, rarity: String, price: Number, sellerId: String, buyerId: String, timestamp: { type: Date, default: Date.now } }));
const Achievements      = model('Achievements',      new Schema({ userId:String, guildId:String, badges:[{id:String,name:String,unlockedAt:Date}], trivia_wins:{type:Number,default:0}, casino_wins:{type:Number,default:0}, win_streak:{type:Number,default:0}, max_bet:{type:Number,default:0}, createdAt:{type:Date,default:Date.now} }));
const MissionProgress   = model('MissionProgress',   new Schema({ userId:String, guildId:String, week:Number, year:Number, missions:[{id:String,name:String,target:Number,progress:{type:Number,default:0},completed:{type:Boolean,default:false},reward:{type:Number,default:300}}], claimed:{type:Boolean,default:false}, claimedAt:Date, createdAt:{type:Date,default:Date.now} }));
const TriviaProgress    = model('TriviaProgress',    new Schema({ userId:String, guildId:String, totalWins:{type:Number,default:0}, totalFails:{type:Number,default:0}, streak:{type:Number,default:0}, triviaCount:{type:Number,default:0}, createdAt:{type:Date,default:Date.now} }));
const Trade             = model('Trade',             new Schema({ guildId:String, fromUserId:String, toUserId:String, fromCoins:{type:Number,default:0}, toCoins:{type:Number,default:0}, status:{type:String,default:'pending'}, expiresAt:Date, createdAt:{type:Date,default:Date.now} }));
const ShopItem          = model('ShopItem',          new Schema({ guildId:String, itemId:String, name:String, description:String, price:Number, emoji:{type:String,default:'🛒'}, active:{type:Boolean,default:true}, createdBy:String, createdAt:{type:Date,default:Date.now} }));
const GiveawayBlacklist = model('GiveawayBlacklist', new Schema({ guildId:String, userId:String, reason:{type:String,default:'Sin razón'}, addedBy:String, addedAt:{type:Date,default:Date.now} }));
const GiveawayConfig    = model('GiveawayConfig',    new Schema({ guildId:{type:String,unique:true}, claimTimeout:{type:Number,default:86400000} }));
const XpBoost           = model('XpBoost',           new Schema({ userId:String, guildId:String, multiplier:Number, expiresAt:Date, grantedBy:String }));
const GameHistory       = model('GameHistory',       new Schema({ guildId:String, game:String, result:mongoose.Schema.Types.Mixed, players:[{userId:String,betType:String,amount:Number,won:Boolean,delta:Number}], endedAt:{type:Date,default:Date.now} }));
const AutoRole          = model('AutoRole',          new Schema({ guildId:String, panelId:{type:String,unique:true}, channelId:String, messageId:String, name:String, title:String, description:String, requireConfirm:{type:Boolean,default:true}, maxRoles:{type:Number,default:0}, roles:[{roleId:String,emoji:String,label:String,description:String,order:Number,minLevel:{type:Number,default:0},requiredRole:String}], createdAt:{type:Date,default:Date.now}, updatedAt:{type:Date,default:Date.now} }));
const ServerBackground  = model('ServerBackground',  new Schema({ guildId:String, bgId:String, name:String, description:String, price:{type:Number,default:0}, filePath:String, isDefault:{type:Boolean,default:false}, createdBy:String, createdAt:{type:Date,default:Date.now} }));
const GuildStyle        = model('GuildStyle',        new Schema({ guildId:{type:String,unique:true}, colors:{primary:Number,success:Number,warning:Number,danger:Number,info:Number,welcome:Number,giveaway:Number,level:Number}, footer:{text:String,iconUrl:String}, welcomeMessage:String, showTimestamp:{type:Boolean,default:true} }));


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

// ── Función auxiliar para obtener stats ────────────────────────────────────
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
      online:  guild?.approximate_presence_count || 0,
      registeredUsers: totalUsers,
      activeGiveaways, activeLoans, flaggedJoins,
      totalWarns, joinsByDay,
      casinoBets:   casinoAgg[0]?.bets   || 0,
      casinoProfit: casinoAgg[0]?.profit  || 0,
    };
  } catch (e) { console.error('[stats]', e); return null; }
}

// ── API: Stats ─────────────────────────────────────────────────────────────
app.get('/api/stats', requireAuth, async (req, res) => {
  const stats = await getStats();
  if (!stats) return res.status(500).json({ error: 'Error' });
  res.json(stats);
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
    res.json({ invites: enriched, pages: Math.ceil(total / limit) });
  } catch (e) { console.error('[invites]', e); res.status(500).json({ error: 'Error' }); }
});

// ── API: Sorteos ───────────────────────────────────────────────────────────
app.get('/api/giveaways', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const status = req.query.status || 'all';
    const limit = 10;
    const query = { guildId: GUILD_ID };
    if (status === 'active') query.ended = false;
    if (status === 'ended') query.ended = true;
    const [agg, countAgg] = await Promise.all([
      Giveaway.find(query).sort({ endsAt: -1 }).skip((page - 1) * limit).limit(limit).lean(),
      Giveaway.countDocuments(query),
    ]);
    const enriched = await Promise.all(agg.map(async g => {
      const u = await getUser(g.hostedBy);
      return { ...g, hostedByName: u.username };
    }));
    res.json({ giveaways: enriched, pages: Math.ceil(countAgg / limit) });
  } catch (e) { console.error('[giveaways]', e); res.status(500).json({ error: 'Error' }); }
});

// ── [REEMPLAZAR] /api/economy ──────────────────────────────────────────────
app.get('/api/economy', requireAuth, async (req, res) => {
  try {
    const page  = parseInt(req.query.page) || 1;
    const sort  = req.query.sort || 'coins';
    const limit = 15;
 
    if (sort === 'backgrounds') {
      const [agg, countAgg] = await Promise.all([
        UserCoins.aggregate([
          { $match: { guildId: GUILD_ID, 'ownedBackgrounds.0': { $exists: true } } },
          { $addFields: { bgCount: { $size: '$ownedBackgrounds' } } },
          { $sort: { bgCount: -1 } },
          { $skip: (page - 1) * limit }, { $limit: limit },
        ]),
        UserCoins.countDocuments({ guildId: GUILD_ID, 'ownedBackgrounds.0': { $exists: true } }),
      ]);
      const enriched = await Promise.all(agg.map(async (u, i) => {
        const user = await getUser(u.userId);
        return { rank: (page-1)*limit+i+1, userId: u.userId, username: user.username, avatar: user.avatar, coins: u.coins, totalEarned: u.totalEarned, ownedBackgrounds: u.ownedBackgrounds||[], activeBgId: u.activeBgId||null, level: 0, xp: 0 };
      }));
      return res.json({ users: enriched, pages: Math.ceil(countAgg / limit) });
    }
 
    const sortObj = sort === 'level' ? { level: -1, xp: -1 } : { coins: -1 };
    const [agg, countAgg] = await Promise.all([
      UserCoins.find({ guildId: GUILD_ID }).sort(sortObj).skip((page-1)*limit).limit(limit).lean(),
      UserCoins.countDocuments({ guildId: GUILD_ID }),
    ]);
    const enriched = await Promise.all(agg.map(async (u, i) => {
      const user = await getUser(u.userId);
      const lvl  = await Level.findOne({ userId: u.userId, guildId: GUILD_ID }).lean();
      return { rank: (page-1)*limit+i+1, userId: u.userId, username: user.username, avatar: user.avatar, coins: u.coins, totalEarned: u.totalEarned, ownedBackgrounds: u.ownedBackgrounds||[], activeBgId: u.activeBgId||null, level: lvl?.level||0, xp: lvl?.xp||0 };
    }));
    res.json({ users: enriched, pages: Math.ceil(countAgg / limit) });
  } catch (e) { console.error('[economy]', e); res.status(500).json({ error: 'Error' }); }
});

// ── API: Préstamos ────────────────────────────────────────────────────────
app.get('/api/loans', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const status = req.query.status || 'all';
    const limit = 15;
    const query = { guildId: GUILD_ID };
    if (status !== 'all') query.status = status;
    const [agg, countAgg] = await Promise.all([
      Loan.find(query).sort({ createdAt: -1 }).skip((page - 1) * limit).limit(limit).lean(),
      Loan.countDocuments(query),
    ]);
    const enriched = await Promise.all(agg.map(async l => {
      const u = await getUser(l.userId);
      return { ...l, username: u.username, avatar: u.avatar };
    }));
    res.json({ loans: enriched, pages: Math.ceil(countAgg / limit) });
  } catch (e) { console.error('[loans]', e); res.status(500).json({ error: 'Error' }); }
});

// ── [REEMPLAZAR] /api/levels ───────────────────────────────────────────────
app.get('/api/levels', requireAuth, async (req, res) => {
  try {
    const page  = parseInt(req.query.page) || 1;
    const limit = 15;
    const [agg, countAgg, dist] = await Promise.all([
      Level.find({ guildId: GUILD_ID }).sort({ level: -1, xp: -1 })
        .skip((page - 1) * limit).limit(limit).lean(),
      Level.countDocuments({ guildId: GUILD_ID }),
      Level.aggregate([
        { $match: { guildId: GUILD_ID } },
        { $bucket: { groupBy: '$level', boundaries: [0, 5, 10, 20, 50, 999], default: '50+', output: { count: { $sum: 1 } } } },
      ]),
    ]);
    const enriched = await Promise.all(agg.map(async (l, i) => {
      const u = await getUser(l.userId);
      return {
        rank: (page - 1) * limit + i + 1,
        userId: l.userId, username: u.username, avatar: u.avatar,
        level: l.level, xp: l.xp,
        streak: l.streak || 0,
        dailyChallenges: l.dailyChallenges || null,
      };
    }));
    const distribution = dist.map(d => ({
      _id: d._id === 0 ? '0' : d._id === 5 ? '1–4' : d._id === 10 ? '5–9' : d._id === 20 ? '10–19' : d._id === 50 ? '20–49' : '50+',
      count: d.count,
    }));
    res.json({ users: enriched, pages: Math.ceil(countAgg / limit), total: countAgg, distribution });
  } catch (e) { console.error('[levels]', e); res.status(500).json({ error: 'Error' }); }
});

// ── [NUEVO] /api/achievements ──────────────────────────────────────────────
app.get('/api/achievements', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1, tab = req.query.tab || 'achievements', limit = 15;
    if (tab === 'achievements') {
      const [agg, count] = await Promise.all([Achievements.find({ guildId: GUILD_ID }).sort({ casino_wins: -1 }).skip((page-1)*limit).limit(limit).lean(), Achievements.countDocuments({ guildId: GUILD_ID })]);
      const enriched = await Promise.all(agg.map(async (a, i) => { const u = await getUser(a.userId); return { rank:(page-1)*limit+i+1, userId:a.userId, username:u.username, avatar:u.avatar, badges:a.badges?.length||0, trivia_wins:a.trivia_wins||0, casino_wins:a.casino_wins||0, win_streak:a.win_streak||0, max_bet:a.max_bet||0 }; }));
      return res.json({ items: enriched, pages: Math.ceil(count/limit) });
    }
    if (tab === 'missions') {
      const now = new Date(), week = Math.ceil(((now - new Date(now.getFullYear(),0,1))/86400000 + new Date(now.getFullYear(),0,1).getDay() + 1) / 7);
      const [agg, count] = await Promise.all([MissionProgress.find({ guildId: GUILD_ID, year: now.getFullYear(), week }).sort({ claimed:1, createdAt:-1 }).skip((page-1)*limit).limit(limit).lean(), MissionProgress.countDocuments({ guildId: GUILD_ID, year: now.getFullYear(), week })]);
      const enriched = await Promise.all(agg.map(async p => { const u = await getUser(p.userId); return { userId:p.userId, username:u.username, avatar:u.avatar, completed:p.missions?.filter(m=>m.completed).length||0, total:p.missions?.length||0, claimed:p.claimed, missions:p.missions }; }));
      return res.json({ items: enriched, pages: Math.ceil(count/limit) });
    }
    if (tab === 'trivia') {
      const [agg, count] = await Promise.all([TriviaProgress.find({ guildId: GUILD_ID }).sort({ totalWins:-1 }).skip((page-1)*limit).limit(limit).lean(), TriviaProgress.countDocuments({ guildId: GUILD_ID })]);
      const enriched = await Promise.all(agg.map(async (t, i) => { const u = await getUser(t.userId); const tot=(t.totalWins||0)+(t.totalFails||0); return { rank:(page-1)*limit+i+1, userId:t.userId, username:u.username, avatar:u.avatar, totalWins:t.totalWins||0, totalFails:t.totalFails||0, streak:t.streak||0, winRate:tot>0?Math.round(t.totalWins/tot*100):0 }; }));
      return res.json({ items: enriched, pages: Math.ceil(count/limit) });
    }
    res.json({ items: [], pages: 0 });
  } catch (e) { console.error('[achievements]', e); res.status(500).json({ error: 'Error' }); }
});

// ── [NUEVO] /api/trades ────────────────────────────────────────────────────
app.get('/api/trades', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, status = req.query.status||'all', limit = 15;
    const query = { guildId: GUILD_ID }; if (status !== 'all') query.status = status;
    const [agg, count] = await Promise.all([Trade.find(query).sort({ createdAt:-1 }).skip((page-1)*limit).limit(limit).lean(), Trade.countDocuments(query)]);
    const enriched = await Promise.all(agg.map(async t => { const from = await getUser(t.fromUserId), to = await getUser(t.toUserId); return { ...t, fromUsername:from.username, fromAvatar:from.avatar, toUsername:to.username, toAvatar:to.avatar }; }));
    res.json({ trades: enriched, pages: Math.ceil(count/limit) });
  } catch (e) { console.error('[trades]', e); res.status(500).json({ error: 'Error' }); }
});

// ── [NUEVO] /api/shop ──────────────────────────────────────────────────────
app.get('/api/shop', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, active = req.query.active, limit = 20;
    const query = { guildId: GUILD_ID }; if (active === 'true') query.active=true; if (active === 'false') query.active=false;
    const [items, count] = await Promise.all([ShopItem.find(query).sort({ price:1 }).skip((page-1)*limit).limit(limit).lean(), ShopItem.countDocuments(query)]);
    const enriched = await Promise.all(items.map(async item => { const u = await getUser(item.createdBy); return { ...item, createdByName:u.username }; }));
    res.json({ items: enriched, pages: Math.ceil(count/limit) });
  } catch (e) { console.error('[shop]', e); res.status(500).json({ error: 'Error' }); }
});

// ── [NUEVO] /api/giveaway-blacklist ───────────────────────────────────────
app.get('/api/giveaway-blacklist', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, limit = 15;
    const [agg, count] = await Promise.all([GiveawayBlacklist.find({ guildId: GUILD_ID }).sort({ addedAt:-1 }).skip((page-1)*limit).limit(limit).lean(), GiveawayBlacklist.countDocuments({ guildId: GUILD_ID })]);
    const enriched = await Promise.all(agg.map(async b => { const u=await getUser(b.userId), a=await getUser(b.addedBy); return { ...b, username:u.username, avatar:u.avatar, addedByName:a.username }; }));
    res.json({ items: enriched, pages: Math.ceil(count/limit) });
  } catch (e) { console.error('[gw-blacklist]', e); res.status(500).json({ error: 'Error' }); }
});

// ── [NUEVO] /api/giveaway-config ──────────────────────────────────────────
app.get('/api/giveaway-config', requireAuth, async (req, res) => {
  try {
    const config = await GiveawayConfig.findOne({ guildId: GUILD_ID }).lean();
    res.json({ config });
  } catch (e) { console.error('[gw-config]', e); res.status(500).json({ error: 'Error' }); }
});

// ── [NUEVO] /api/xpboosts ─────────────────────────────────────────────────
app.get('/api/xpboosts', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, limit = 15, now = new Date();
    const [agg, count] = await Promise.all([XpBoost.find({ guildId: GUILD_ID, expiresAt:{ $gt:now } }).sort({ expiresAt:1 }).skip((page-1)*limit).limit(limit).lean(), XpBoost.countDocuments({ guildId: GUILD_ID, expiresAt:{ $gt:now } })]);
    const enriched = await Promise.all(agg.map(async b => { const u=await getUser(b.userId), g=await getUser(b.grantedBy); return { ...b, username:u.username, avatar:u.avatar, grantedByName:g.username, hoursLeft:Math.max(0,Math.floor((new Date(b.expiresAt)-now)/3600000)) }; }));
    res.json({ items: enriched, pages: Math.ceil(count/limit) });
  } catch (e) { console.error('[xpboosts]', e); res.status(500).json({ error: 'Error' }); }
});
 
// ── [NUEVO] /api/games ────────────────────────────────────────────────────
app.get('/api/games', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, game = req.query.game||'all', limit = 15;
    const query = { guildId: GUILD_ID }; if (game !== 'all') query.game = game;
    const [agg, count, topGames] = await Promise.all([
      GameHistory.find(query).sort({ endedAt:-1 }).skip((page-1)*limit).limit(limit).lean(),
      GameHistory.countDocuments(query),
      GameHistory.aggregate([{ $match:{ guildId:GUILD_ID } }, { $group:{ _id:'$game', count:{ $sum:1 }, totalPot:{ $sum:{ $sum:'$players.amount' } } } }, { $sort:{ count:-1 } }]),
    ]);
    const enriched = await Promise.all(agg.map(async g => {
      const players = await Promise.all((g.players||[]).slice(0,5).map(async p => { const u=await getUser(p.userId); return { ...p, username:u.username }; }));
      return { ...g, players };
    }));
    res.json({ games: enriched, pages: Math.ceil(count/limit), topGames });
  } catch (e) { console.error('[games]', e); res.status(500).json({ error: 'Error' }); }
});

// ── [NUEVO] /api/autoroles ────────────────────────────────────────────────
app.get('/api/autoroles', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, limit = 15;
    const [agg, count] = await Promise.all([AutoRole.find({ guildId: GUILD_ID }).sort({ updatedAt:-1 }).skip((page-1)*limit).limit(limit).lean(), AutoRole.countDocuments({ guildId: GUILD_ID })]);
    res.json({ items: agg, pages: Math.ceil(count/limit) });
  } catch (e) { console.error('[autoroles]', e); res.status(500).json({ error: 'Error' }); }
});
 
// ── [NUEVO] /api/backgrounds ──────────────────────────────────────────────
app.get('/api/backgrounds', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page)||1, filter = req.query.filter||'all', limit = 20;
    const query = { guildId: GUILD_ID };
    if (filter === 'default') query.isDefault = true;
    if (filter === 'custom')  query.isDefault = false;
    const [items, count] = await Promise.all([ServerBackground.find(query).sort({ createdAt:-1 }).skip((page-1)*limit).limit(limit).lean(), ServerBackground.countDocuments(query)]);
    const enriched = await Promise.all(items.map(async bg => { const u=await getUser(bg.createdBy); return { ...bg, createdByName:u.username }; }));
    res.json({ items: enriched, pages: Math.ceil(count/limit) });
  } catch (e) { console.error('[backgrounds]', e); res.status(500).json({ error: 'Error' }); }
});

// ── API: Moderación ───────────────────────────────────────────────────────
app.get('/api/moderation', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const tab = req.query.tab || 'warns';
    const limit = 15;
    let items = [];
    if (tab === 'warns') {
      const agg = await Warn.find({ guildId: GUILD_ID }).sort({ createdAt: -1 }).skip((page - 1) * limit).limit(limit).lean();
      items = await Promise.all(agg.map(async w => {
        const u = await getUser(w.userId);
        return { ...w, username: u.username, avatar: u.avatar, modName: w.moderator };
      }));
    } else if (tab === 'mutes') {
      const agg = await Mute.find({ guildId: GUILD_ID }).sort({ createdAt: -1 }).skip((page - 1) * limit).limit(limit).lean();
      items = await Promise.all(agg.map(async m => {
        const u = await getUser(m.userId);
        return { ...m, username: u.username, avatar: u.avatar };
      }));
    } else if (tab === 'logs') {
      const agg = await SecurityLog.find({ guildId: GUILD_ID }).sort({ joinedAt: -1 }).skip((page - 1) * limit).limit(limit).lean();
      items = await Promise.all(agg.map(async l => {
        const u = await getUser(l.userId);
        return { ...l, username: u.username, avatar: u.avatar };
      }));
    }
    const count = tab === 'warns' ? await Warn.countDocuments({ guildId: GUILD_ID }) : tab === 'mutes' ? await Mute.countDocuments({ guildId: GUILD_ID }) : await SecurityLog.countDocuments({ guildId: GUILD_ID });
    res.json({ items, pages: Math.ceil(count / limit) });
  } catch (e) { console.error('[moderation]', e); res.status(500).json({ error: 'Error' }); }
});

// ── API: Bans ──────────────────────────────────────────────────────────────
app.get('/api/bans', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = 15;
    const bans = await Warn.find({ guildId: GUILD_ID, reason: { $regex: 'ban', $options: 'i' } }).sort({ createdAt: -1 }).skip((page - 1) * limit).limit(limit).lean();
    const enriched = await Promise.all(bans.map(async b => {
      const u = await getUser(b.userId);
      return { ...b, username: u.username, avatar: u.avatar };
    }));
    const count = await Warn.countDocuments({ guildId: GUILD_ID, reason: { $regex: 'ban', $options: 'i' } });
    res.json({ bans: enriched, pages: Math.ceil(count / limit) });
  } catch (e) { console.error('[bans]', e); res.status(500).json({ error: 'Error' }); }
});

// ── API: Casino ────────────────────────────────────────────────────────────
app.get('/api/casino', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const game = req.query.game || 'all';
    const limit = 15;
    const query = { guildId: GUILD_ID };
    if (game !== 'all') query.game = game;
    const [logs, countAgg, topWinners, topLosers] = await Promise.all([
      CasinoLog.find(query).sort({ timestamp: -1 }).skip((page - 1) * limit).limit(limit).lean(),
      CasinoLog.countDocuments(query),
      CasinoLog.aggregate([{ $match: { guildId: GUILD_ID, profit: { $gt: 0 } } }, { $group: { _id: '$userId', totalProfit: { $sum: '$profit' }, count: { $sum: 1 } } }, { $sort: { totalProfit: -1 } }, { $limit: 5 }]),
      CasinoLog.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: '$userId', netProfit: { $sum: '$profit' }, count: { $sum: 1 } } }, { $sort: { netProfit: 1 } }, { $limit: 5 }]),
    ]);
    const enrichedLogs = await Promise.all(logs.map(async l => {
      const u = await getUser(l.userId);
      return { ...l, username: u.username, avatar: u.avatar };
    }));
    const enrichedWinners = await Promise.all(topWinners.map(async w => {
      const u = await getUser(w._id);
      return { ...w, username: u.username, avatar: u.avatar };
    }));
    const enrichedLosers = await Promise.all(topLosers.map(async l => {
      const u = await getUser(l._id);
      return { ...l, username: u.username, avatar: u.avatar };
    }));
    const activity = {};
    for (let i = 6; i >= 0; i--) {
      const d = new Date(); d.setDate(d.getDate() - i);
      const key = d.toLocaleDateString('es-PE', { weekday: 'short', day: 'numeric' });
      activity[key] = { count: 0 };
    }
    logs.forEach(l => {
      const key = new Date(l.timestamp).toLocaleDateString('es-PE', { weekday: 'short', day: 'numeric' });
      if (activity[key]) activity[key].count++;
    });
    const stats = await CasinoLog.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: null, totalBets: { $sum: 1 }, totalBetAmount: { $sum: '$bet' }, houseProfit: { $sum: '$profit' } } }]);
    const totalWins = await CasinoLog.countDocuments({ guildId: GUILD_ID, ...(game !== 'all' ? { game } : {}), profit: { $gt: 0 } });
    const winRate = stats[0]?.totalBets ? Math.round((totalWins / stats[0].totalBets) * 100) : 0;
    res.json({ logs: enrichedLogs, topWinners: enrichedWinners, topLosers: enrichedLosers, activity, stats: { ...stats[0], winRate }, pages: Math.ceil(countAgg / limit) });
  } catch (e) { console.error('[casino]', e); res.status(500).json({ error: 'Error' }); }
});

// ── API: Armas ─────────────────────────────────────────────────────────────
app.get('/api/arms', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const tab = req.query.tab || 'inventory';
    const rarity = req.query.rarity || 'all';
    const limit = 15;
    const query = { guildId: GUILD_ID };
    if (rarity !== 'all') query.rarity = rarity;
    if (tab === 'inventory') {
      const items = await Arm.find(query).sort({ obtainedAt: -1 }).skip((page - 1) * limit).limit(limit).lean();
      const enriched = await Promise.all(items.map(async a => {
        const u = await getUser(a.userId);
        return { ...a, username: u.username, avatar: u.avatar };
      }));
      const count = await Arm.countDocuments(query);
      res.json({ items: enriched, pages: Math.ceil(count / limit) });
    } else if (tab === 'prices') {
      const items = await ArmCurrentPrice.find({ guildId: GUILD_ID }).sort({ currentPrice: -1 }).skip((page - 1) * limit).limit(limit).lean();
      const count = await ArmCurrentPrice.countDocuments({ guildId: GUILD_ID });
      res.json({ items, pages: Math.ceil(count / limit) });
    } else if (tab === 'cases') {
      const recentOpens = await Arm.find(query).sort({ obtainedAt: -1 }).limit(limit).lean();
      const enriched = await Promise.all(recentOpens.map(async a => {
        const u = await getUser(a.userId);
        return { ...a, username: u.username, avatar: u.avatar };
      }));
      res.json({ recentOpens: enriched });
    } else if (tab === 'collectors') {
      const collectors = await Arm.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: '$userId', total: { $sum: 1 }, totalValue: { $sum: '$sellPrice' }, special: { $sum: { $cond: [{ $eq: ['$rarity', 'special'] }, 1, 0] } }, covert: { $sum: { $cond: [{ $eq: ['$rarity', 'covert'] }, 1, 0] } }, classified: { $sum: { $cond: [{ $eq: ['$rarity', 'classified'] }, 1, 0] } } } }, { $sort: { totalValue: -1 } }, { $skip: (page - 1) * limit }, { $limit: limit }]);
      const enriched = await Promise.all(collectors.map(async (c, i) => {
        const u = await getUser(c._id);
        return { rank: (page - 1) * limit + i + 1, userId: c._id, username: u.username, avatar: u.avatar, ...c };
      }));
      const count = await Arm.distinct('userId', { guildId: GUILD_ID });
      res.json({ items: enriched, pages: Math.ceil(count.length / limit) });
    }
  } catch (e) { console.error('[arms]', e); res.status(500).json({ error: 'Error' }); }
});

// ── API: Mercado ───────────────────────────────────────────────────────────
app.get('/api/market', requireAuth, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const tab = req.query.tab || 'listings';
    const limit = 15;
    if (tab === 'listings') {
      const items = await MarketListing.find({ guildId: GUILD_ID, status: 'active' }).sort({ listedAt: -1 }).skip((page - 1) * limit).limit(limit).lean();
      const enriched = await Promise.all(items.map(async l => {
        const u = await getUser(l.sellerId);
        return { ...l, sellerName: u.username, sellerAvatar: u.avatar };
      }));
      const count = await MarketListing.countDocuments({ guildId: GUILD_ID, status: 'active' });
      return res.json({ items: enriched, pages: Math.ceil(count / limit) });
    }

    if (tab === 'history') {
      const txs = await MarketTx.find({ guildId: GUILD_ID }).sort({ timestamp: -1 }).skip((page - 1) * limit).limit(limit).lean();
      const enriched = await Promise.all(txs.map(async t => {
        const buyer = await getUser(t.buyerId);
        const seller = await getUser(t.sellerId);
        return { ...t, buyerName: buyer.username, sellerName: seller.username };
      }));
      const count = await MarketTx.countDocuments({ guildId: GUILD_ID });
      return res.json({ items: enriched, pages: Math.ceil(count / limit) });
    }

    if (tab === 'chart') {
      const itemId = req.query.itemId;
      const topItems = await ArmCurrentPrice.find({ guildId: GUILD_ID }).sort({ currentPrice: -1 }).limit(10).lean();
      if (!itemId || !topItems.find(i => i.itemId === itemId)) {
        return res.json({ topItems, history: [], current: null });
      }
      const history = await ArmPrice.find({ guildId: GUILD_ID, itemId }).sort({ timestamp: -1 }).limit(100).lean();
      const current = await ArmCurrentPrice.findOne({ guildId: GUILD_ID, itemId }).lean();
      return res.json({ topItems, history: history.reverse(), current });
    }

    if (tab === 'traders') {
      const d = await MarketTx.aggregate([{ $match: { guildId: GUILD_ID, timestamp: { $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) } } }, { $group: { _id: '$buyerId', spent: { $sum: '$price' }, count: { $sum: 1 } } }, { $sort: { spent: -1 } }, { $limit: 10 }]);
      const topBuyers = await Promise.all(d.map(async b => {
        const u = await getUser(b._id);
        return { ...b, username: u.username, avatar: u.avatar };
      }));
      const s = await MarketTx.aggregate([{ $match: { guildId: GUILD_ID, timestamp: { $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) } } }, { $group: { _id: '$sellerId', earned: { $sum: '$price' }, count: { $sum: 1 } } }, { $sort: { earned: -1 } }, { $limit: 10 }]);
      const topSellers = await Promise.all(s.map(async s => {
        const u = await getUser(s._id);
        return { ...s, username: u.username, avatar: u.avatar };
      }));
      const totalVolume = await MarketTx.aggregate([{ $match: { guildId: GUILD_ID } }, { $group: { _id: null, total: { $sum: '$price' } } }]);
      return res.json({ topBuyers, topSellers, totalVolume: totalVolume[0]?.total || 0 });
    }

    res.json({ items: [], total: 0, pages: 0 });
  } catch (e) { console.error('[market]', e); res.status(500).json({ error: 'Error' }); }
});

// ── [REEMPLAZAR] /api/config ───────────────────────────────────────────────
app.get('/api/config', requireAuth, async (req, res) => {
  try {
    const [channels, rewards, style] = await Promise.all([
      ChannelsConfig.findOne({ guildId: GUILD_ID }).lean(),
      RewardsConfig.findOne({ guildId: GUILD_ID }).lean(),
      GuildStyle.findOne({ guildId: GUILD_ID }).lean(),
    ]);
    res.json({ channels, rewards, style });
  } catch (e) { console.error('[config]', e); res.status(500).json({ error: 'Error' }); }
});

// ── WebSocket: Autenticación y eventos ─────────────────────────────────────
io.use((socket, next) => {
  next();
});

io.on('connection', (socket) => {
  console.log(`[WS] Usuario conectado: ${socket.id}`);
  
  socket.on('disconnect', () => {
    console.log(`[WS] Usuario desconectado: ${socket.id}`);
  });
});

// Función para emitir actualizaciones en tiempo real
global.emitUpdate = (event, data) => {
  io.emit(event, data);
};

// ── Actualización periódica de stats en tiempo real ────────────────────────
setInterval(async () => {
  const stats = await getStats();
  if (stats) io.emit('stats:update', stats);
}, 5000); // Actualiza cada 5 segundos

// ── Páginas ────────────────────────────────────────────────────────────────
app.get('/login',     (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/dashboard', (req, res) => { if (!req.session?.user) return res.redirect('/login'); res.sendFile(path.join(__dirname, 'public', 'dashboard.html')); });
app.get('/',          (req, res) => res.redirect(req.session?.user ? '/dashboard' : '/login'));

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`[DASHBOARD] Puerto ${PORT}`));
