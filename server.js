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

// ── Session ────────────────────────────────────────────────────────────────
app.use(session({
  secret:            process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex'),
  resave:            false,
  saveUninitialized: false,
  cookie:            { secure: false, maxAge: 24 * 60 * 60 * 1000 } // 24h
}));

// ── MongoDB ────────────────────────────────────────────────────────────────
mongoose.connect(process.env.MONGO_URI).then(() => console.log('[DB] MongoDB conectado')).catch(console.error);

// ── Schemas (reutiliza los del bot) ────────────────────────────────────────
const { Schema, model } = mongoose;

const GiveawaySchema = new Schema({ guildId: String, channelId: String, messageId: String, prize: String, winners: Number, hostedBy: String, endsAt: Date, ended: Boolean, participants: [String], winnerIds: [String], requiredRole: String, minLevel: Number, minInvites: Number, claimDeadline: Date, claimedBy: [String] });
const UserCoinsSchema = new Schema({ userId: String, guildId: String, coins: Number, totalEarned: Number, lastDaily: Date, lastWork: Date });
const LevelSchema     = new Schema({ userId: String, guildId: String, xp: Number, level: Number });
const WarnSchema      = new Schema({ userId: String, guildId: String, moderator: String, reason: String, createdAt: Date });
const MuteSchema      = new Schema({ userId: String, guildId: String, expiresAt: Date, reason: String });
const SecurityLogSchema = new Schema({ userId: String, guildId: String, ip: String, country: String, countryCode: String, action: String, flagged: Boolean, joinedAt: Date });

const Giveaway    = model('Giveaway',    GiveawaySchema);
const UserCoins   = model('UserCoins',   UserCoinsSchema);
const Level       = model('Level',       LevelSchema);
const Warn        = model('Warn',        WarnSchema);
const Mute        = model('Mute',        MuteSchema);
const SecurityLog = model('SecurityLog', SecurityLogSchema);

// ── Discord OAuth2 helpers ─────────────────────────────────────────────────
const DISCORD_CLIENT_ID     = process.env.DISCORD_CLIENT_ID;
const DISCORD_CLIENT_SECRET = process.env.DISCORD_CLIENT_SECRET;
const DISCORD_REDIRECT_URI  = process.env.DISCORD_REDIRECT_URI;
const GUILD_ID              = process.env.GUILD_ID;
// Soporta múltiples roles separados por coma en ADMIN_ROLE_IDS
// Ejemplo: ADMIN_ROLE_IDS=123456789,987654321
const ADMIN_ROLE_IDS = (process.env.ADMIN_ROLE_IDS || process.env.ADMIN_ROLE_ID || '')
  .split(',').map(r => r.trim()).filter(Boolean);

function discordGet(endpoint, token) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'discord.com',
      path:     `/api/v10${endpoint}`,
      method:   'GET',
      headers:  { 'Authorization': `Bearer ${token}` }
    }, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => { try { resolve(JSON.parse(body)); } catch { resolve(null); } });
    });
    req.on('error', reject);
    req.end();
  });
}

function discordBotGet(endpoint) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'discord.com',
      path:     `/api/v10${endpoint}`,
      method:   'GET',
      headers:  { 'Authorization': `Bot ${process.env.DISCORD_TOKEN}` }
    }, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => { try { resolve(JSON.parse(body)); } catch { resolve(null); } });
    });
    req.on('error', reject);
    req.end();
  });
}

function exchangeCode(code) {
  return new Promise((resolve, reject) => {
    const params = new URLSearchParams({
      client_id:     DISCORD_CLIENT_ID,
      client_secret: DISCORD_CLIENT_SECRET,
      grant_type:    'authorization_code',
      code,
      redirect_uri:  DISCORD_REDIRECT_URI,
    }).toString();

    const req = https.request({
      hostname: 'discord.com',
      path:     '/api/v10/oauth2/token',
      method:   'POST',
      headers:  { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(params) }
    }, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => { try { resolve(JSON.parse(body)); } catch { resolve(null); } });
    });
    req.on('error', reject);
    req.write(params);
    req.end();
  });
}

// ── Auth middleware ────────────────────────────────────────────────────────
function requireAuth(req, res, next) {
  if (!req.session?.user) return res.status(401).json({ error: 'No autenticado' });
  next();
}

// ── OAuth2 Routes ──────────────────────────────────────────────────────────
app.get('/auth/discord', (req, res) => {
  const url = `https://discord.com/oauth2/authorize?client_id=${DISCORD_CLIENT_ID}&redirect_uri=${encodeURIComponent(DISCORD_REDIRECT_URI)}&response_type=code&scope=identify+guilds.members.read`;
  res.redirect(url);
});

app.get('/auth/callback', async (req, res) => {
  const { code } = req.query;
  if (!code) return res.redirect('/login?error=no_code');

  try {
    const tokens = await exchangeCode(code);
    if (!tokens?.access_token) return res.redirect('/login?error=token_failed');

    const user   = await discordGet('/users/@me', tokens.access_token);
    if (!user?.id) return res.redirect('/login?error=user_failed');

    // Verifica que el usuario está en el servidor y tiene rol de admin
    const member = await discordBotGet(`/guilds/${GUILD_ID}/members/${user.id}`);
    if (!member?.roles) return res.redirect('/login?error=not_in_server');
    const hasRole = member.roles.some(r => ADMIN_ROLE_IDS.includes(r));
    if (!hasRole) return res.redirect('/login?error=no_permission');

    req.session.user = {
      id:       user.id,
      username: user.username,
      avatar:   user.avatar ? `https://cdn.discordapp.com/avatars/${user.id}/${user.avatar}.png` : `https://cdn.discordapp.com/embed/avatars/0.png`,
    };

    res.redirect('/dashboard');
  } catch (e) {
    console.error('[AUTH]', e);
    res.redirect('/login?error=server_error');
  }
});

app.get('/auth/logout', (req, res) => {
  req.session.destroy();
  res.redirect('/login');
});

app.get('/api/me', requireAuth, (req, res) => {
  res.json(req.session.user);
});

// ── API: Estadísticas generales ───────────────────────────────────────────
app.get('/api/stats', requireAuth, async (req, res) => {
  try {
    const [guildData, totalCoins, totalLevels, recentLogs, activeGiveaways] = await Promise.all([
      discordBotGet(`/guilds/${GUILD_ID}?with_counts=true`),
      UserCoins.countDocuments({ guildId: GUILD_ID }),
      Level.countDocuments({ guildId: GUILD_ID }),
      SecurityLog.find({ guildId: GUILD_ID }).sort({ joinedAt: -1 }).limit(30),
      Giveaway.countDocuments({ guildId: GUILD_ID, ended: false }),
    ]);

    // Joins por día (últimos 7 días)
    const joinsByDay = {};
    const now = new Date();
    for (let i = 6; i >= 0; i--) {
      const d = new Date(now);
      d.setDate(d.getDate() - i);
      joinsByDay[d.toLocaleDateString('es', { weekday: 'short', day: 'numeric' })] = 0;
    }
    recentLogs.forEach(log => {
      const d = new Date(log.joinedAt);
      const key = d.toLocaleDateString('es', { weekday: 'short', day: 'numeric' });
      if (joinsByDay[key] !== undefined) joinsByDay[key]++;
    });

    res.json({
      members:         guildData?.approximate_member_count || 0,
      online:          guildData?.approximate_presence_count || 0,
      registeredUsers: totalCoins,
      levelsTracked:   totalLevels,
      activeGiveaways,
      joinsByDay,
      flaggedJoins:    recentLogs.filter(l => l.flagged).length,
    });
  } catch (e) {
    console.error('[API/stats]', e);
    res.status(500).json({ error: 'Error interno' });
  }
});

// ── API: Sorteos ──────────────────────────────────────────────────────────
app.get('/api/giveaways', requireAuth, async (req, res) => {
  try {
    const { status = 'all', page = 1 } = req.query;
    const filter = { guildId: GUILD_ID };
    if (status === 'active') filter.ended = false;
    if (status === 'ended')  filter.ended = true;

    const [giveaways, total] = await Promise.all([
      Giveaway.find(filter).sort({ endsAt: -1 }).skip((page - 1) * 10).limit(10),
      Giveaway.countDocuments(filter),
    ]);

    // Enriquece con usernames de Discord
    const enriched = await Promise.all(giveaways.map(async g => {
      const hostUser = await discordBotGet(`/users/${g.hostedBy}`).catch(() => null);
      return {
        ...g.toObject(),
        hostedByName: hostUser?.username || g.hostedBy,
        hostedByAvatar: hostUser?.avatar ? `https://cdn.discordapp.com/avatars/${g.hostedBy}/${hostUser.avatar}.png?size=32` : null,
      };
    }));

    res.json({ giveaways: enriched, total, pages: Math.ceil(total / 10) });
  } catch (e) {
    res.status(500).json({ error: 'Error interno' });
  }
});

app.delete('/api/giveaways/:messageId', requireAuth, async (req, res) => {
  try {
    await Giveaway.findOneAndUpdate({ messageId: req.params.messageId, guildId: GUILD_ID }, { ended: true });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: 'Error interno' });
  }
});

// ── API: Economía ─────────────────────────────────────────────────────────
app.get('/api/economy', requireAuth, async (req, res) => {
  try {
    const { sort = 'coins', page = 1 } = req.query;
    const sortField = sort === 'level' ? { level: -1, xp: -1 } : { coins: -1 };

    let users;
    if (sort === 'level') {
      users = await Level.find({ guildId: GUILD_ID }).sort(sortField).skip((page - 1) * 15).limit(15);
    } else {
      users = await UserCoins.find({ guildId: GUILD_ID }).sort(sortField).skip((page - 1) * 15).limit(15);
    }

    const total = sort === 'level'
      ? await Level.countDocuments({ guildId: GUILD_ID })
      : await UserCoins.countDocuments({ guildId: GUILD_ID });

    const enriched = await Promise.all(users.map(async (u, i) => {
      const discordUser = await discordBotGet(`/users/${u.userId}`).catch(() => null);
      return {
        rank:     (page - 1) * 15 + i + 1,
        userId:   u.userId,
        username: discordUser?.username || u.userId,
        avatar:   discordUser?.avatar ? `https://cdn.discordapp.com/avatars/${u.userId}/${discordUser.avatar}.png?size=32` : null,
        coins:    u.coins || 0,
        level:    u.level || 0,
        xp:       u.xp || 0,
        totalEarned: u.totalEarned || 0,
      };
    }));

    res.json({ users: enriched, total, pages: Math.ceil(total / 15) });
  } catch (e) {
    res.status(500).json({ error: 'Error interno' });
  }
});

app.get('/api/economy/user/:userId', requireAuth, async (req, res) => {
  try {
    const { userId } = req.params;
    const [coins, level, warns, mute, discordUser] = await Promise.all([
      UserCoins.findOne({ userId, guildId: GUILD_ID }),
      Level.findOne({ userId, guildId: GUILD_ID }),
      Warn.find({ userId, guildId: GUILD_ID }).sort({ createdAt: -1 }),
      Mute.findOne({ userId, guildId: GUILD_ID }),
      discordBotGet(`/users/${userId}`),
    ]);

    res.json({
      userId,
      username:    discordUser?.username || userId,
      avatar:      discordUser?.avatar ? `https://cdn.discordapp.com/avatars/${userId}/${discordUser.avatar}.png?size=64` : null,
      coins:       coins?.coins || 0,
      totalEarned: coins?.totalEarned || 0,
      level:       level?.level || 0,
      xp:          level?.xp || 0,
      warns:       warns.length,
      muted:       !!mute,
    });
  } catch (e) {
    res.status(500).json({ error: 'Error interno' });
  }
});

// ── API: Moderación ───────────────────────────────────────────────────────
app.get('/api/moderation', requireAuth, async (req, res) => {
  try {
    const { tab = 'warns', page = 1 } = req.query;

    if (tab === 'warns') {
      const [warns, total] = await Promise.all([
        Warn.find({ guildId: GUILD_ID }).sort({ createdAt: -1 }).skip((page - 1) * 15).limit(15),
        Warn.countDocuments({ guildId: GUILD_ID }),
      ]);
      const enriched = await Promise.all(warns.map(async w => {
        const [u, mod] = await Promise.all([
          discordBotGet(`/users/${w.userId}`).catch(() => null),
          discordBotGet(`/users/${w.moderator}`).catch(() => null),
        ]);
        return { ...w.toObject(), username: u?.username || w.userId, modName: mod?.username || w.moderator };
      }));
      return res.json({ items: enriched, total, pages: Math.ceil(total / 15) });
    }

    if (tab === 'mutes') {
      const [mutes, total] = await Promise.all([
        Mute.find({ guildId: GUILD_ID }).sort({ _id: -1 }).skip((page - 1) * 15).limit(15),
        Mute.countDocuments({ guildId: GUILD_ID }),
      ]);
      const enriched = await Promise.all(mutes.map(async m => {
        const u = await discordBotGet(`/users/${m.userId}`).catch(() => null);
        return { ...m.toObject(), username: u?.username || m.userId };
      }));
      return res.json({ items: enriched, total, pages: Math.ceil(total / 15) });
    }

    if (tab === 'logs') {
      const [logs, total] = await Promise.all([
        SecurityLog.find({ guildId: GUILD_ID }).sort({ joinedAt: -1 }).skip((page - 1) * 15).limit(15),
        SecurityLog.countDocuments({ guildId: GUILD_ID }),
      ]);
      const enriched = await Promise.all(logs.map(async l => {
        const u = await discordBotGet(`/users/${l.userId}`).catch(() => null);
        return { ...l.toObject(), username: u?.username || l.userId };
      }));
      return res.json({ items: enriched, total, pages: Math.ceil(total / 15) });
    }

    res.json({ items: [], total: 0, pages: 0 });
  } catch (e) {
    res.status(500).json({ error: 'Error interno' });
  }
});

// ── Páginas ────────────────────────────────────────────────────────────────
app.get('/login', (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/dashboard', (req, res) => {
  if (!req.session?.user) return res.redirect('/login');
  res.sendFile(path.join(__dirname, 'public', 'dashboard.html'));
});
app.get('/', (req, res) => res.redirect(req.session?.user ? '/dashboard' : '/login'));

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`[DASHBOARD] Corriendo en puerto ${PORT}`));
