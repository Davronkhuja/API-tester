# ⚡ API Runner

> Postman Runner-ga o'xshash, lekin yengil va tez — CSV/JSON fayldan ma'lumot olib, har bir qatorni API ga donalab yuboradi va natijalarni real vaqtda ko'rsatadi.

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat-square&logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.0+-000000?style=flat-square&logo=flask&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-22c55e?style=flat-square)

---

## Imkoniyatlar

| | |
|---|---|
| **Batch yuborish** | CSV yoki JSON fayldan ma'lumot olib, har bir qatorni API ga donalab yuboradi |
| **`{{o'zgaruvchi}}`** | URL, header va body ichidagi o'zgaruvchilarni avtomatik almashtiradi |
| **Real-time natijalar** | Server-Sent Events (SSE) orqali har bir so'rov natijasi darhol ko'rinadi |
| **CURL parse** | URL maydoniga CURL buyrug'ini joylashtiring — barcha parametrlar avtomatik to'ldiriladi |
| **Fayl ixtiyoriy** | Faylsiz ham bitta so'rov yuborish mumkin |
| **Saqlangan so'rovlar** | So'rovlarni papkalar bilan tartiblab saqlash, qidirish imkoni |
| **Split-pane interfeys** | Natijalar o'ng panelda ochiladi, ajratuvchini sudrab o'lcham o'zgartirish mumkin |
| **RUN / STOP** | Istalgan vaqtda batch jarayonni to'xtatish imkoni |

---

## O'rnatish

```bash
git clone https://github.com/Davronkhuja/API-tester.git
cd API-tester
pip install -r requirements.txt
python api_runner.py
```

Brauzer avtomatik ochiladi: **http://127.0.0.1:5050**

---

## Ishlatish

### Oddiy so'rov

1. Method (`GET`, `POST`, `PUT`...) va URL kiriting
2. Kerak bo'lsa **Params**, **Headers**, **Auth** yoki **Body** tablarini to'ldiring
3. **▶ RUN** tugmasini bosing

### Batch yuborish (Runner rejimi)

1. CSV yoki JSON fayl yuklang
2. URL, header yoki body ichida `{{ustun_nomi}}` yozing
3. **▶ RUN** — har bir satr uchun alohida so'rov yuboriladi

**CSV misol:**
```csv
user_id,token
101,abc123
102,def456
```

**URL misol:**
```
https://api.example.com/users/{{user_id}}
```

**Body misol:**
```json
{
  "id": "{{user_id}}",
  "auth": "{{token}}"
}
```

### CURL paste

URL maydoniga to'liq CURL buyrug'ini joylashtiring — method, headerlar, body va auth avtomatik aniqlanadi:

```bash
curl -X POST 'https://api.example.com/data' \
  -H 'Authorization: Bearer mytoken' \
  -H 'Content-Type: application/json' \
  -d '{"key": "value"}'
```

### So'rovlarni saqlash

- Sidebar dagi **+ Yangi so'rov** orqali so'rovni saqlang
- **📁 Papka** tugmasi bilan papkalar yarating va so'rovlarni tartiblab joylashtiring
- Keyingi safar bir bosish bilan qayta yuklang

---

## Texnologiyalar

- **Backend:** Python 3 / Flask
- **Real-time:** Server-Sent Events (SSE)
- **Frontend:** Vanilla JS + CSS (framework yo'q, zero dependencies)
- **Saqlash:** `saved_requests.json` (lokal fayl)

---

## Fayl tuzilmasi

```
API-tester/
├── api_runner.py        # Asosiy fayl (backend + frontend)
├── requirements.txt     # Python kutubxonalari
├── saved_requests.json  # Saqlangan so'rovlar (gitignore'd)
└── README.md
```
