
//1
def espar(num:Int):Boolean ={return (num%2==0)}
var num = 0
println(espar(num))

//2
val list = List(1,2,3,8)
if(list(2)%2==0){println("true")}else{println(false)}

//3
def suerte(list:List(Int)):Int={
  var res=0
  for (n <- list){
    if(n==7){
      res=res+14
    }else{
      res = res+n
    }
  }
  return res
}


//5
def palin(palabra:String):Boolean ={
  return (palabra==palabra.reverse)
}
 val palabra = "oso"
 val palabra2 = "casa"

 println(palin(palabra))
 println(palin(palabra2))
