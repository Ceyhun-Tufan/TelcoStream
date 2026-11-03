-- KEYS[1]: Rate limit anahtarı (örneğin: ratelimit:subscriber:5321112233)
-- ARGV[1]: Şu anki zaman damgası (milisaniye)
-- ARGV[2]: Pencere boyutu (milisaniye, örn: 1000 = 1 saniye)
-- ARGV[3]: İzin verilen maksimum istek sayısı (örn: 5)

local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

local clear_before = now - window

-- 1. Süresi dolmuş eski istekleri temizle
redis.call('ZREMRANGEBYSCORE', key, 0, clear_before)

-- 2. Mevcut penceredeki istek sayısını al
local current_requests = redis.call('ZCARD', key)

-- 3. Limit kontrolü
if current_requests < limit then
    -- Benzersiz bir üye eklemek için zaman damgası ve rastgele bir sayı kullanıyoruz
    local member = now .. ":" .. redis.call('TIME')[2]
    redis.call('ZADD', key, now, member)

    -- Anahtarın hafızada sonsuza kadar kalmaması için TTL (ömür) ata
    redis.call('PEXPIRE', key, window)
    return 1 -- İzin verildi (Allowed)
else
    return 0 -- Limit aşıldı (Rejected)
end
