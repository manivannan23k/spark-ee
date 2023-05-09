package com.gishorizon.operations

import breeze.linalg.DenseMatrix
import breeze.linalg.svd
import com.gishorizon.Spark
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.Vectors

object FpcaDev {


  def normalCompute(arr: Array[IndexedRow]): (DenseMatrix[Double], DenseMatrix[Double]) = {
    try{
      val N = arr.length
      val _N = arr.length
      val _T = arr(0).vector.toArray.length
      val matrix: DenseMatrix[Double] = new DenseMatrix(_N, _T, arr.flatMap {
        a =>
          a.vector.toArray
      })

      val scale: DenseMatrix[Double] = new DenseMatrix(N, N, (0 until _N).map { i => {
        (0 until _N).map {
          j => {
            if (i == j) {
              (1 / Math.sqrt(_T)).toDouble
            } else {
              0.0
            }
          }
        }
      }
      }.toList.toArray.flatten)
      val e: DenseMatrix[Double] = scale.*(matrix)
      val _svd = svd(e)
      val V: DenseMatrix[Double] = new DenseMatrix(N, N, _svd.U.toArray)
      val s = _svd.S.data
      val S: DenseMatrix[Double] = new DenseMatrix(N, 1, (0 until _N).map { i => {
        (0 until _N).map {
          j => {
            if (i == j) {
              s(i)
            } else {
              0.0
            }
          }
        }
      }
      }.toList.toArray.flatten)
      val Si: DenseMatrix[Double] = new DenseMatrix(N, 1, (0 until _N).map { i => {
        (0 until _N).map {
          j => {
            if (i == j) {
              1 / s(i)
            } else {
              0.0
            }
          }
        }
      }
      }.toList.toArray.flatten)
      val scores = V * S
      val t = V * Si
      val mt = matrix.t
      val components = mt * t
      val FPCAVals = components * (scores.t)
      (components, FPCAVals)
    }
    catch {
      case (e) => {
//        println("Error FPCA")
        (null, null)
      }
    }
  }
  def distCompute(arr: Array[IndexedRow]): (BlockMatrix, BlockMatrix) ={
    implicit val sc = Spark.context
    val matrix = new IndexedRowMatrix(sc.parallelize(arr)).toBlockMatrix().cache()
    val N = matrix.numRows().toInt
    val L = 1

    val _N = matrix.numRows().toInt
    val _T = matrix.numCols().toInt
    val _t: List[Int] = (0 until _N).map { i => {
      i.toInt
    }
    }.toList
    val scale = new CoordinateMatrix(
      sc.parallelize(_t)
        .map {
          i => {
            MatrixEntry(i, i, 1 / Math.sqrt(_T))
          }
        },
      _N,
      _N
    ).toBlockMatrix()
    val svd = scale.multiply(matrix).transpose.toIndexedRowMatrix().computeSVD(L)
    val s = svd.s
    val Va = svd.V
    var Vl = Array[IndexedRow]()
    for (i <- 0 until N) {
      Vl = Vl :+ IndexedRow(i, Vectors.dense(
        Va.toArray(L * i + 0),
        //        Va.toArray(L * i + 1)
      ))
    }
    val V = new IndexedRowMatrix(sc.parallelize(Vl), N, L)
    val S = new linalg.DenseMatrix(L, L, org.apache.spark.mllib.linalg.Matrices.diag(s).toArray)
    val Si = new linalg.DenseMatrix(L, L, org.apache.spark.mllib.linalg.Matrices.diag(
      Vectors.dense(s.toArray.map(e => 1 / e))
    ).toArray)
    val scores = V.multiply(S)
    val t = V.multiply(Si).toBlockMatrix()
    val mt = matrix.transpose
    val components = mt.multiply(t)
    val FPCA = components.multiply(scores.toBlockMatrix().transpose)
    (components, FPCA)
  }

}
