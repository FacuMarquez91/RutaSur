'use client';import {useEffect,useState} from 'react';
export default function Live({initial,kind,render}){const [data,setData]=useState(initial);useEffect(()=>{let es=new EventSource(`/api/live?kind=${encodeURIComponent(kind)}`);es.onmessage=x=>{try{setData(JSON.parse(x.data))}catch{}};return()=>es.close()},[kind]);return render(data)}
