# NOAH PROJECT - MODULE 4: SECURITY GATEWAY

Muc tieu module nay la an toan bo service ben duoi va chi mo mot cong duy nhat:

- Gateway: `http://localhost:8000`
- Order API: chi chay noi bo trong Docker network, khong expose `5000`
- Report Service: chi chay noi bo trong Docker network, khong expose `5001`

## Chay project

```powershell
docker compose up -d --build
```

## Gateway routes

Tat ca request phai gui header:

```text
apikey: noah-secret-key
```

### Orders

```powershell
curl.exe -H "apikey: noah-secret-key" http://localhost:8000/orders
```

Tao order:

```powershell
python -c "import json, urllib.request; data=json.dumps({'user_id':2,'product_id':102,'quantity':1}).encode(); req=urllib.request.Request('http://localhost:8000/orders', data=data, headers={'Content-Type':'application/json','apikey':'noah-secret-key'}, method='POST'); res=urllib.request.urlopen(req); print(res.status); print(res.read().decode())"
```

### Report

```powershell
curl.exe -H "apikey: noah-secret-key" http://localhost:8000/report
```

## Kiem tra bao mat

Khong co API key se bi chan:

```powershell
curl.exe -i http://localhost:8000/orders
```

Goi truc tiep service se khong duoc vi `docker-compose.yml` khong expose port `5000`:

```powershell
curl.exe -i http://localhost:5000/orders
```

Chi Kong co `ports`:

```yaml
ports:
  - "8000:8000"
```

## Cau hinh Kong

File `kong.yml` dang cau hinh:

- `/orders` -> `order-api:5000`
- `/report` -> `report-service:5001`
- Key Authentication voi key `noah-secret-key`
- Rate Limiting: 10 request/phut moi consumer
- CORS: bat san cho frontend
